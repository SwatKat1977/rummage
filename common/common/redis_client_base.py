"""
Module Name: redis_client_base.py
Description: Wrapper class for the redis client library

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
import redis
import redis.exceptions

DEFAULT_HOST : str = 'localhost'
DEFAULT_PORT : int = 6379

class RedisClientBase:
    ''' Wrapper for the Redis client library '''

    def __init__(self) -> None:
        self._client = None
        self._host = DEFAULT_HOST
        self._port = DEFAULT_PORT

    def connect(self,
                hostname : str = DEFAULT_HOST,
                port_number : int = DEFAULT_PORT,
                password: str = None) -> bool:
        """
        Connect to a Redis server.

        This method establishes a connection to a Redis server using the
        provided hostname and port number. If the connection is successful, it
        sets the internal host and port attributes and returns True. If the
        connection fails, it raises a RuntimeError with the reason for the
        failure.

        Args:
            hostname (str): The hostname or IP address of the Redis server.
                            Defaults to `DEFAULT_HOST`.

            port_number (int): The port number on which the Redis server is
                               listening. Defaults to `DEFAULT_PORT`.

            password (str, optional): The password for the Redis server, if
                                      required. Defaults to None.

        Returns:
            bool: True if the connection to the Redis server is successful, 
                False otherwise.

        Raises:
            RuntimeError: If the connection to the Redis server fails, this
                        exception is raised with a message containing the
                        hostname, port number, and the reason for the failure.
        """
        status : bool = False

        try:
            # Connect to Redis server
            self._client = redis.Redis(host=hostname,
                                       port=port_number,
                                       password=password)

            # Test the connection by pinging the server
            if self._client.ping():
                self._host = hostname
                self._port = port_number
                status = True

        except redis.ConnectionError as exception:
            # Raise the exception with a meaningful message
            raise RuntimeError(
                f"Failed to connect to Redis at {hostname}:{port_number}, " +
                f"reason: {exception}") from exception

        return status

    def disconnect(self):
        """
        Disconnect from the Redis server.

        This method closes the connection to the Redis server if a client
        instance exists. While it's not strictly necessary to explicitly
        disconnect, calling this method can help manage resources in scenarios
        where maintaining an open connection is not required.
        """

        # You don't need to explicitly disconnect, but you can close the
        # connection if necessary
        if self._client:
            self._client.close()

    def set_hash_field_value(self, key : any, field: str, value: any) -> None:
        """
        Sets the value of a specified field in a Redis hash.

        This method uses the Redis HSET command to store the provided value
        for the given field under the specified key. If the key does not 
        exist, a new hash is created.

        Args:
            key (any): The key under which the hash is stored. 
                    It can be of any type that is accepted as a key in Redis.
            field (str): The field within the hash to set the value for.
            value (any): The value to assign to the specified field. 
                        It can be of any type that can be serialized to 
                        a string, such as a string, integer, or float.

        Raises:
            RuntimeError: If there is no connection to the Redis server 
                        when attempting to set the field value.

        Example:
            >>> obj.setHashFieldValue('user:1001', 'name', 'Alice')
            >>> obj.setHashFieldValue('user:1001', 'age', 30)

        Notes:
            - Ensure that the Redis client (`self._client`) is initialized 
            and connected to the Redis server before calling this method.
            - The value will be converted to a string format by Redis. 
            If the value is not a string, make sure it is serializable.
        """

        if not self._client:
            raise RuntimeError("No connection to Redis server")

        self._set_hash_field_value(key, field, value)

    def set_hash_field_values(self, key : any, field_values : dict) -> None:
        """
        Sets multiple field values in a hash stored at a given key in Redis.

        This method iterates over a list of field-value pairs and sets each
        field's value in the specified hash. It first checks for a valid
        connection to the Redis server before proceeding with the updates.

        Args:
            key (any): The key under which the hash is stored in Redis. This
                        can be of any type, as Redis keys can be strings or
                        bytes.
            field_values (list): A list of tuples, where each tuple contains a
                                field name (str) and its corresponding value
                                (any) to be set in the hash.

        Raises:
            RuntimeError: If there is no connection to the Redis server or if
                        any issues occur while setting field values via the
                        _set_hash_field_value method.
        """

        if not self._client:
            raise RuntimeError("No connection to Redis server")

        self._set_hash_field_mapping(key, field_values)

    def get_hash_field_value(self, key : any, field: str) -> any:
        """
        Retrieve the value of a field from a Redis hash.

        This method retrieves the value associated with the given `field`
        from a Redis hash stored at the specified `key`.

        Args:
            key (any): 
                The key of the Redis hash from which to retrieve the field value.
                This can be any data type supported by Redis, typically a string.
            
            field (str): 
                The field within the Redis hash whose value needs to be retrieved.

        Returns:
            any: 
                The value associated with the given `field` in the Redis hash,
                or None if the field does not exist.

        Raises:
            redis.RedisError: 
                If there is an issue retrieving the field value from the Redis server.
        """
        return self._client.hget(key, field)

    def set_field_value(self, key: any, value: any) -> None:
        """
        Set the value of a specific field (key) in the Redis database.

        This checks if there is an active connection to the Redis server.
        If the connection is available, it will set the value of the provided
        key in the Redis database. If the connection is not established, it
        raises a RuntimeError.

        Args:
            key (any): The key corresponding to the field in Redis.
            value (any): The value to be set for the given key.

        Raises:
            RuntimeError: If there is no active connection to the Redis server.

        Returns:
            None
        """

        if not self._client:
            raise RuntimeError("No connection to Redis server")

        self._client.set(key, value)

    def get_field_value(self, key: any) -> any:
        """
        Retrieve the value of a specific field (key) from the Redis database.

        This checks if there is an active connection to the Redis server.
        If the connection is available, it fetches the value of the provided
        key from the Redis database. If the connection is not established, it
        raises a RuntimeError.

        Args:
            key (any): The key corresponding to the field in Redis.

        Raises:
            RuntimeError: If there is no active connection to the Redis server.

        Returns:
            any: The value stored in Redis for the provided key. Returns None
            if the key does not exist.
        """
        if not self._client:
            raise RuntimeError("No connection to Redis server")

        return self._client.get(key)

    def field_exists(self, key) -> bool:
        """
        Check if a specific field (key) exists in the Redis database.

        This checks if there is an active connection to the Redis server.
        If the connection is available, it verifies whether the provided key
        exists in the Redis database. If the connection is not established,
        it raises a RuntimeError.

        Args:
            key (any): The key corresponding to the field in Redis.

        Raises:
            RuntimeError: If there is no active connection to the Redis server.

        Returns:
            bool: True if the key exists in Redis, False otherwise.
        """
        if not self._client:
            raise RuntimeError("No connection to Redis server")

        return self._client.exists(key)

    def increment_field_value(self, key: any) -> int:
        """
        Increments the value associated with the given key in Redis.

        This method checks if there is an active Redis connection and if the
        specified key exists before attempting to increment its value. If the
        key exists, it increments its value by 1 and returns the new value. If
        the key does not exist, it returns None.

        Args:
            key (any): The key whose value is to be incremented. This is
                    expected to be a string that corresponds to a field stored
                    in the Redis database.

        Returns:
            int: The incremented value of the field after the operation.
            None: If the field does not exist or there is no connection to
                  Redis.

        Raises:
            RuntimeError: If there is no active connection to the Redis server.
        """
        if not self._client:
            raise RuntimeError("No connection to Redis server")

        if not self.field_exists(key):
            return None

        try:
            return self._client.incr(key)

        except redis.exceptions.ResponseError as ex:
            raise RuntimeError(f"ResponseError thrown: {ex}") from ex

    def _set_hash_field_value(self, key : any, field: str, value: any) -> None:
        """
        Sets the value of a specified field in a hash stored at a given key in
        Redis.

        This method uses the Redis client to set a field's value in a hash.
        It handles potential errors that may arise during the operation,
        such as connection issues, response errors, data errors, and other
        generic exceptions.

        Args:
            key (any): The key under which the hash is stored in Redis. This
                        can be of any type, as Redis keys can be strings or
                        bytes.
            field (str): The field within the hash for which the value needs to
                        be set. This must be a string representing the field
                        name.
            value (any): The value to set for the specified field. This can be
                        of any type, as Redis supports various data types.

        Raises:
            RuntimeError: If there is a connection issue with the Redis server,
                        a data exception is caught, or a generic exception
                        occurs.
        """
        try:
            self._client.hset(key, field, value)

        except (redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError,
                redis.exceptions.TimeoutError) as ex:
            raise RuntimeError(f"Connection issue with Redis server: {ex}") from ex

        except (redis.exceptions.DataError) as ex:
            raise RuntimeError(f"Data exception caught: {ex}") from ex

        except Exception as ex:
            raise RuntimeError(f"Generic exception caught: {ex}") from ex

    def _set_hash_field_mapping(self, key, value: dict) -> None:
        try:
            self._client.hset(key, mapping=value)

        except (redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError,
                redis.exceptions.TimeoutError) as ex:
            raise RuntimeError(f"Connection issue with Redis server: {ex}") from ex

        except (redis.exceptions.DataError) as ex:
            raise RuntimeError(f"Data exception caught: {ex}") from ex

        except Exception as ex:
            raise RuntimeError(f"Generic exception caught: {ex}") from ex
