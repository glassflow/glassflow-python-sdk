from __future__ import annotations

from .client import APIClient
from .models import api, operations


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

        Returns:
            Space: Space object to interact with the GlassFlow API

        Raises:
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
