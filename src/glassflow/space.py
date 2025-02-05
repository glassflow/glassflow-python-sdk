from __future__ import annotations

from pathlib import PurePosixPath

import requests

from .client import APIClient
from .models import errors
from .models.api.v2 import CreateSpace
from .models.operations.v2 import CreateSpaceResponse


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
        self.request_headers = {"Personal-Access-Token": self.personal_access_token}
        self.request_query_params = {"organization_id": self.organization_id}

    def create(self) -> Space:
        """
        Creates a new GlassFlow space

        Returns:
            self: Space object

        Raises:
            ValueError: If name is not provided in the constructor

        """
        create_space = CreateSpace(name=self.name).model_dump(mode="json")
        endpoint = "/spaces"

        http_res = self._request2(method="POST", endpoint=endpoint, body=create_space)
        content_type = http_res.headers.get("Content-Type")
        res = CreateSpaceResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
            body=http_res.json(),
        )

        self.id = res.body.id
        self.created_at = res.body.created_at
        self.name = res.body.name
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
        self._request2(method="DELETE", endpoint=endpoint)

    def _request2(
        self,
        method,
        endpoint,
        request_headers=None,
        body=None,
        request_query_params=None,
    ) -> requests.Response:
        headers = self._get_headers2()
        headers.update(self.request_headers)
        if request_headers:
            headers.update(request_headers)
        query_params = self.request_query_params

        if request_query_params:
            query_params.update(request_query_params)
        url = (
            f"{self.glassflow_config.server_url.rstrip('/')}/{PurePosixPath(endpoint)}"
        )
        try:
            http_res = self.client.request(
                method, url=url, params=query_params, headers=headers, json=body
            )
            http_res.raise_for_status()
            return http_res
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 401:
                raise errors.UnauthorizedError(http_err.response)
            if http_err.response.status_code == 404:
                raise errors.SpaceNotFoundError(self.id, http_err.response)
            if http_err.response.status_code == 409:
                raise errors.SpaceIsNotEmptyError(http_err.response)
            # TODO add Unknown Error for 400 and 500
            raise http_err
