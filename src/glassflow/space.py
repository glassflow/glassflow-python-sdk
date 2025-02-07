from __future__ import annotations

import requests
from .client import APIClient
from .models import errors
from .models.api import v2

from dataclasses import is_dataclass


class Space(APIClient):
    def __init__(
        self,
        personal_access_token: str,
        name: str | None = None,
        id: str | None = None,
        created_at: str | None = None,
        organization_id: str | None = None,
    ):
        """Creates a new GlassFlow space object

        Args:
            personal_access_token: The personal access token to authenticate
                against GlassFlow
            name: Name of the space
            id: ID of the GlassFlow Space you want to create the pipeline in
            created_at: Timestamp when the space was created

        """
        super().__init__()
        self.name = name
        self.id = id
        self.created_at = created_at
        self.organization_id = organization_id
        self.personal_access_token = personal_access_token
        self.headers = {"Personal-Access-Token": self.personal_access_token}
        self.query_params = {"organization_id": self.organization_id}

    def create(self) -> Space:
        """
        Creates a new GlassFlow space

        Returns:
            self: Space object

        Raises:
            ValueError: If name is not provided in the constructor

        """
        space_api_obj = v2.CreateSpace(name=self.name)
        print(is_dataclass(space_api_obj))  # True

        endpoint = "/spaces"
        http_res = self._request(method="POST", endpoint=endpoint, json=space_api_obj.model_dump())

        space_created = v2.Space(**http_res.json())
        self.id = space_created.id
        self.created_at = space_created.created_at
        self.name = space_created.name
        return self

    def delete(self) -> None:
        """
        Deletes a GlassFlow space

        Returns:

        Raises:
            ValueError: If ID is not provided in the constructor
            SpaceNotFoundError: If ID provided does not match any
                existing space in GlassFlow
            UnauthorizedError: If the Personal Access Token is not
                provided or is invalid
        """
        if self.id is None:
            raise ValueError("Space id must be provided in the constructor")

        endpoint = f"/spaces/{self.id}"
        self._request(method="DELETE", endpoint=endpoint)

    def _request(
        self,
        method,
        endpoint,
        request_headers=None,
        json=None,
        request_query_params=None,
        files=None,
        data=None
    ):
        headers = {**self.headers, **(request_headers or {})}
        query_params = {**self.query_params, **(request_query_params or {})}
        try:
            return super()._request(
                method=method, endpoint=endpoint, request_headers=headers, json=json, request_query_params=query_params, files=files, data=data
            )
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 401:
                raise errors.UnauthorizedError(http_err.response)
            if http_err.response.status_code == 404:
                raise errors.SpaceNotFoundError(self.id, http_err.response)
            if http_err.response.status_code == 409:
                raise errors.SpaceIsNotEmptyError(http_err.response)
            # TODO add Unknown Error for 400 and 500
            raise http_err
