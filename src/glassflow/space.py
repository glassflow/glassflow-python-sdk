from __future__ import annotations

from .client import APIClient
from .models import api, errors, operations


class Space(APIClient):
    def __init__(
        self,
        personal_access_token: str,
        name: str | None = None,
        id: str | None = None,
        created_at: str | None = None,
        organization_id: str | None = None,
    ):
        """Creates a new GlassFlow pipeline object

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

    def create(self) -> Space:
        """
        Creates a new GlassFlow space

        Returns:
            self: Space object

        Raises:
            ValueError: If name is not provided in the constructor

        """
        if self.name is None:
            raise ValueError("Name must be provided in order to create the space")
        create_space = api.CreateSpace(name=self.name)
        request = operations.CreateSpaceRequest(
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
            **create_space.__dict__,
        )
        base_res = self._request(method="POST", endpoint="/spaces", request=request)

        res = operations.CreateSpaceResponse(
            status_code=base_res.status_code,
            content_type=base_res.content_type,
            raw_response=base_res.raw_response,
            **base_res.raw_response.json(),
        )

        self.id = res.id
        self.created_at = res.created_at
        self.name = res.name
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

        request = operations.DeleteSpaceRequest(
            space_id=self.id,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )
        self._request(
            method="DELETE",
            endpoint=f"/spaces/{self.id}",
            request=request,
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        request: operations.BaseManagementRequest,
        **kwargs,
    ) -> operations.BaseResponse:
        try:
            return super()._request(
                method=method, endpoint=endpoint, request=request, **kwargs
            )
        except errors.ClientError as e:
            if e.status_code == 404:
                raise errors.SpaceNotFoundError(self.id, e.raw_response) from e
            elif e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            elif e.status_code == 409:
                raise errors.SpaceIsNotEmptyError(e.raw_response) from e
            else:
                raise e
