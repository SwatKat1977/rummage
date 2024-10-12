"""
Module Name: scraper_redis_client.py
Description: Scraper specific redis client

Copyright (C) 2024 Rummage Contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
import logging
import time
import redis
from common.redis_client_base import RedisClientBase

class ScraperRedisClient(RedisClientBase):
    """
    ScraperRedisClient is a client class for interacting with a Redis database, specifically 
    designed for a web scraping context. It inherits from `RedisClientBase` and adds logging 
    functionality and Redis initialization with optional forceful reinitialization.

    Attributes:
    -----------
    KEY_DOMAIN_ENTRY_ID : str
        Class constant that holds the Redis key for storing the domain entry ID.

    KEY_NODE_ASSIGMENTS : str
        I am a lot of shit

    Methods:
    --------
    __init__(logger : logging.Logger):
        Initializes a new instance of ScraperRedisClient. Takes a logger as a parameter 
        and assigns it to an instance-specific logger for detailed logging.

    initialise_redis(force = False) -> bool:
        Initializes the Redis connection. Logs the initialization process. 
        Can optionally force the reinitialization if `force` is set to True.
    """

    KEY_DOMAIN_ENTRY_ID : str = "domain_entry_id"
    DOMAIN_ENTRY_BY_TIMESTAMP_SET : str = "domain_entries_by_timestamp"
    DOMAIN_ENTRY_ASSIGNED_TO_NODE_SET : str = "domain_entries_assigned_to_node"
    KEY_DOMAIN_ENTRY_PREFIX : str = "NODE_ENTRY:"

    DOMAIN_ASSIGNMENT_STATUS_ASSIGNED  = "assigned"
    DOMAIN_ASSIGNMENT_STATUS_UNASSIGNED  = "unassigned"

    def __init__(self, logger : logging.Logger):
        super().__init__()
        self._logger = logger.getChild(__name__)

    def initialise_redis(self, force = False) -> bool:
        """
        Initializes Redis by checking and setting up the necessary keys and
        data.

        This method performs various checks on specific Redis keys and manages
        the data according to the `force` parameter. It ensures that the domain
        entry ID and node assignments set are properly initialized, and can
        reset or delete existing data when required.

        Args:
            force (bool, optional): If True, existing entries will be reset or 
            deleted. Defaults to False.

        Returns:
            bool: True if Redis initialization is successful.

        Important Notes:
            - If `force` is set to True, the function resets the domain entry ID
            and clears existing domain entries and node assignments set.
            - Be cautious when re-initializing as existing entries may be
            overwritten or cleared.
        
        Logs:
            - Information messages are logged at various steps of the process.
        
        """
        self._logger.info("Initialising Redis...")

        self._logger.info("=> Checking key '%s'...", self.KEY_DOMAIN_ENTRY_ID)
        if not self.field_exists(self.KEY_DOMAIN_ENTRY_ID):
            self._logger.info("   Key '%s' is not set, setting...",
                              self.KEY_DOMAIN_ENTRY_ID)
            self.set_field_value(self.KEY_DOMAIN_ENTRY_ID, 0)
        else:
            if force:
                self._logger.info("   Key '%s' is being reset to 0!",
                                  self.KEY_DOMAIN_ENTRY_ID)
                self.set_field_value(self.KEY_DOMAIN_ENTRY_ID, 0)
            else:
                self._logger.info("   Key '%s' already exists...",
                                  self.KEY_DOMAIN_ENTRY_ID)

        self._logger.info("=> Checking for domain entries...")
        entries_found: int = self.count_keys_with_prefix(
            self.KEY_DOMAIN_ENTRY_PREFIX)
        self._logger.info("   Total entries found: %d", entries_found)
        if force:
            self._logger.info("   Deleting all entries...")
            self.delete_keys_with_prefix(self.KEY_DOMAIN_ENTRY_PREFIX)

        self._logger.info("=> Checking '%s' sorted set...",
                          self.DOMAIN_ENTRY_BY_TIMESTAMP_SET)
        if self.field_exists(self.DOMAIN_ENTRY_BY_TIMESTAMP_SET):
            if force:
                self._logger.info("   Set exists, clearing due to 'force' set")
                self.clear_sorted_set(self.DOMAIN_ENTRY_BY_TIMESTAMP_SET)
            else:
                self._logger.info("   Set exists...")
        else:
            self._logger.info("   Set does not exists, will be created on add")

    def add_domain_entry(self, domain : str) -> None:
        """
        Adds a new domain entry to the system with an associated timestamp,
        status, and assignment.

        This function creates a new entry for the given domain, assigns it a
        unique ID, and stores the entry in a hash. The entry is also added to a
        sorted set based on its timestamp, ensuring that unassigned domain
        entries are ordered by when they were added.

        Args:
            domain (str): The domain URL to be added as an entry.

        The following actions are performed:
            1. A current timestamp is generated and used for the entry.
            2. The domain is assigned an 'assigned' status and left unassigned
               to any node.
            3. A unique entry ID is generated for the domain.
            4. The entry is stored as a hash with the generated key.
            5. The entry is added to a sorted set of unassigned entries,
               ordered by timestamp.
        """

        timestamp : int = int(time.time())
        data : dict = {
            'URL': domain,
            'timestamp': timestamp,
            'assigned_status': self.DOMAIN_ASSIGNMENT_STATUS_UNASSIGNED,
            'node_assignment': ''
        }

        entry_id : int = self.increment_field_value(self.KEY_DOMAIN_ENTRY_ID)
        key_id : str = f"{self.KEY_DOMAIN_ENTRY_PREFIX}{entry_id}"

        # Store the new domain entry in the hash
        self.set_hash_field_values(key_id, data)

        # Add the entry to the sorted set of unassigned entries
        self.add_to_sorted_set(self.DOMAIN_ENTRY_BY_TIMESTAMP_SET,
                               {key_id: timestamp})

    def find_and_assign_oldest_entry(self):
        while True:
            # Get the oldest entries sorted by timestamp
            entries = self._client.zrangebyscore(
                self.DOMAIN_ENTRY_BY_TIMESTAMP_SET, '-inf', '+inf')

            for entry_key in entries:
                # Watch the entry to ensure no other client modifies it
                self._client.watch(entry_key)

                # Check if the entry is still unassigned
                status = self._client.hget(entry_key, 'assigned_status').decode()

                if status == self.DOMAIN_ASSIGNMENT_STATUS_UNASSIGNED:
                    # Start a transaction
                    pipe = self._client.pipeline()
                    pipe.multi()

                    # Mark the entry as assigned
                    pipe.hset(entry_key, 'assigned_status',
                              self.DOMAIN_ASSIGNMENT_STATUS_ASSIGNED)

                    # Remove it from unassigned sorted set
                    pipe.zrem(self.DOMAIN_ENTRY_BY_TIMESTAMP_SET, entry_key)

                    # Add it to the 'assigned' sorted set
                    timestamp = int(self._client.hget(entry_key, 'timestamp'))
                    pipe.zadd(self.DOMAIN_ENTRY_ASSIGNED_TO_NODE_SET,
                              {entry_key: timestamp})

                    try:
                        # Execute the transaction
                        pipe.execute()

                        # If transaction succeeds, return the entry
                        url = self._client.hget(entry_key, 'URL').decode()
                        print(f"Locked and moved entry: {entry_key.decode()} "
                              f"with URL: {url}, Timestamp: {timestamp}")
                        return

                    except redis.WatchError:
                        # If the transaction fails due to modification by another client, retry
                        continue

                # Unwatch the entry if status is not 'unassigned'
                self._client.unwatch()

            print("No unassigned entries found.")
            return
