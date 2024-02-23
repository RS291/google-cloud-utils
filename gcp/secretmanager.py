from logging import Logger
from typing import Optional

from google.cloud import secretmanager


class SecretManagerClient(object):
    """
    A client for creating, updating, and retrieving GCP secrets.

    TODO: confirm logging messages are not exposing secret values.
    """

    def __init__(self, logger: Logger, project_id: Optional[str] = None):
        self._client = secretmanager.SecretManagerServiceClient()
        self._logger = logger
        self._project_id = project_id

    def get_secret_from_name(
        self,
        name: str,
    ):
        """
        DEPRECATED: Use `access_secret_version` for all new code.
        Use the full name of the secret (i.e. `projects/*/secrets/*/versions/*`) to retrieve a secret

        name : full name of the secret to get from GCP
        """
        self._logger.warning(f"SecretManagerClient.get_secret_from_name is deprecated")
        self._logger.info(f"Attempting to access: {self._project_id} - {name}...")
        response = self._client.access_secret_version(
            request={"name": name}
        ).payload.data.decode("utf-8")
        self._logger.info(f"Accessed secret: {self._project_id} - {name}.")
        return response

    def create_secret(
        self,
        secret_id: str,
        project_id: Optional[str] = None,
    ):
        """
        Create a new secret in the specified project

        project_id : GCP project in which to store secrets
        secret_id : ID of the secret to create

        """
        project_id = project_id if project_id is not None else self._project_id
        assert project_id is not None
        self._logger.info(
            f"Attempting to create new secret: {project_id} - {secret_id}..."
        )
        parent = f"projects/{project_id}"
        secret = self._client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        self._logger.info(f"Created secret: {secret.name}.")
        return

    def add_secret_version(
        self, secret_id: str, secret_payload: str, project_id: Optional[str] = None
    ):
        """
        Add new secret version to the given secret with the provided payload.
        Taken from GCP documentation.

        project_id : GCP project in which to store secrets
        secret_id : ID of the secret to create
        secret_payload : payload for new secret version as a string

        """
        project_id = project_id if project_id is not None else self._project_id
        assert project_id is not None
        self._logger.info(
            f"Attempting to create new secret version: {project_id} - {secret_id}..."
        )
        parent = self._client.secret_path(project_id, secret_id)
        payload = secret_payload.encode("UTF-8")
        response = self._client.add_secret_version(
            request={"parent": parent, "payload": {"data": payload}}
        )
        self._logger.info(f"Added secret version: {response.name}.")
        return

    def access_secret_version(
        self, secret_id: str, version_id: str = None, project_id: Optional[str] = None
    ):
        """
        Access the secret for given secret version if one exists.
        Can be given as version number or an alias (e.g. "5" or "latest")
        Default behavior returns "latest" secret version.

        project_id : GCP project in which to store secrets
        secret_id : ID of the secret to access
        version_id : version ID of secret to retrieve
        """
        version_id = "latest" if version_id is None else version_id
        project_id = project_id if project_id is not None else self._project_id
        assert project_id is not None
        self._logger.info(f"Attempting to access: {project_id} - {secret_id}...")
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self._client.access_secret_version(
            request={"name": name}
        ).payload.data.decode("UTF-8")
        self._logger.info(f"Accessed secret version: {name}.")
        return response

    def delete_secret(self, secret_id, project_id: Optional[str] = None):
        """
        Delete a secret

        secret_id: ID of the secret to delete
        project_id: ID of the project, if not specified before"""
        project_id = project_id if project_id is not None else self._project_id
        assert project_id is not None
        self._logger.info(f"Attempting to delete secret: {project_id} - {secret_id}...")
        parent = self._client.secret_path(project_id, secret_id)
        self._client.delete_secret(name=parent)
        self._logger.info(f"Deleted secret: {project_id} - {secret_id}.")

    def list_secret_versions(
        self,
        secret_id: str,
        project_id: Optional[str] = None,
        return_all: Optional[bool] = False,
    ) -> list[str]:
        """List all secret versions.

        Args:
            secret_id: ID of the secret to delete
            project_id: ID of the project, if not specified before
            return_all: return all versions, including deactivated ones,
                if true, only active versions if false. Default false.

        Returns:
            list of version IDs as strings
        """
        project_id = project_id if project_id is not None else self._project_id
        assert project_id is not None
        # define enabled
        enabled = secretmanager.SecretVersion.State.ENABLED
        result_iter = self._client.list_secret_versions(
            parent=self._client.secret_path(project_id, secret_id)
        )
        output = []
        for result in result_iter:
            version_id = (
                result.name.split("/")[-1]
                if (return_all or result.state == enabled)
                else None
            )
            if version_id:
                output.append(version_id)
        return output


if __name__ == "__example__":
    import logging

    smc = SecretManagerClient(logging.getLogger(), "")
    secret_id = "uuid-classifier-service-string"
    version_list = smc.list_secret_versions(secret_id)
    for v in version_list:
        smc.access_secret_version(secret_id, v)
