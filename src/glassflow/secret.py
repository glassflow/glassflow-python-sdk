from __future__ import annotations

import re

from .api_client import APIClient
from .models import api, errors


class Secret(APIClient):
    def __init__(
        self,
        personal_access_token: str,
        key: str | None = None,
        value: str | None = None,
        organization_id: str | None = None,
    ):
        """
        Creates a new Glassflow Secret object

        Args:
            personal_access_token: The personal access token to authenticate
                against GlassFlow
            key: Name of the secret. It must start with a letter,
                and it can only contain characters in a-zA-Z0-9_
            value: Value of the secret to store

        Raises:
            SecretInvalidKeyError: If secret key is invalid
        """
        super().__init__()
        self.personal_access_token = personal_access_token
        self.organization_id = organization_id
        self.key = key
        self.value = value
        self.headers = {"Personal-Access-Token": self.personal_access_token}
        self.query_params = {"organization_id": self.organization_id}

        if self.key and not self._is_key_valid(self.key):
            raise errors.SecretInvalidKeyError(self.key)

    def create(self) -> Secret:
        """
        Creates a new Glassflow Secret

        Returns:
            self: Secret object

        Raises:
            ValueError: If secret key or value are not set in the constructor
            Unauthorized: If personal access token is invalid
        """
        if self.key is None:
            raise ValueError("Secret key is required in the constructor")
        if self.value is None:
            raise ValueError("Secret value is required in the constructor")

        secret_api_obj = api.CreateSecret(
            **{
                "key": self.key,
                "value": self.value,
            }
        )

        endpoint = "/secrets"
        self._request(
            method="POST", endpoint=endpoint, json=secret_api_obj.model_dump()
        )
        return self

    def delete(self):
        """
        Deletes a Glassflow Secret.

        Returns:

        Raises:
            Unauthorized: If personal access token is invalid
            SecretNotFound: If secret key does not exist
            ValueError: If secret key is not set in the constructor
        """
        if self.key is None:
            raise ValueError("Secret key is required in the constructor")

        endpoint = f"/secrets/{self.key}"
        self._request(method="DELETE", endpoint=endpoint)

    @staticmethod
    def _is_key_valid(key, search=re.compile(r"[^a-zA-Z0-9_]").search):
        return not bool(search(key))

    def _request(
        self,
        method,
        endpoint,
        request_headers=None,
        json=None,
        request_query_params=None,
        files=None,
        data=None,
    ):
        headers = {**self.headers, **(request_headers or {})}
        query_params = {**self.query_params, **(request_query_params or {})}
        try:
            return super()._request(
                method=method,
                endpoint=endpoint,
                request_headers=headers,
                json=json,
                request_query_params=query_params,
                files=files,
                data=data,
            )
        except errors.UnknownError as e:
            if e.status_code == 401:
                raise errors.SecretUnauthorizedError(self.key, e.raw_response) from e
            if e.status_code == 404:
                raise errors.SecretNotFoundError(self.key, e.raw_response) from e
            raise e
