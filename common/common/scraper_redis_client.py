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
    KEY_DOMAIN_ENTRY_BY_TIMESTAMP_SET : str = "domain_entry_by_timestamp"
    KEY_DOMAIN_ENTRY_PREFIX : str = "NODE_ENTRY:"

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
                          self.KEY_DOMAIN_ENTRY_BY_TIMESTAMP_SET)
        if self.field_exists(self.KEY_DOMAIN_ENTRY_BY_TIMESTAMP_SET):
            if force:
                self._logger.info("   Set exists, clearing due to 'force' set")
                self.clear_sorted_set(self.KEY_DOMAIN_ENTRY_BY_TIMESTAMP_SET)
            else:
                self._logger.info("   Set exists...")
        else:
            self._logger.info("   Set does not exists, will be created on add")

    def find_and_assign_oldest_entry(self):
        while True:
            # Get the oldest entries sorted by timestamp
            entries = self._client.zrangebyscore('entries_by_timestamp',
                                                 '-inf',
                                                 '+inf')

            for entry_key in entries:
                # Watch the entry to ensure no other client modifies it
                self._client.watch(entry_key)
                print(f"Key : {entry_key}")

                # Check if the entry is still unassigned
                status = self._client.hget(entry_key, 'assigned_status').decode()
                if status == 'unassigned':
                    # Start a transaction
                    pipe = self._client.pipeline()
                    pipe.multi()

                    # Mark the entry as assigned
                    pipe.hset(entry_key, 'assigned_status', 'assigned')

                    # Remove it from unassigned sorted set
                    pipe.zrem('entries_by_timestamp', entry_key)

                    # Add it to assigned sorted set
                    timestamp = int(self._client.hget(entry_key, 'timestamp'))
                    pipe.zadd('assigned_entries', {entry_key: timestamp})

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
