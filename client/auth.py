"""Auth Client"""

from typing import Optional
import grpc
from passlib.hash import pbkdf2_sha256
from client.protocol import ProtocolClient as CurpClient
from api.xline.xline_command_pb2 import Command, RequestWithToken, CommandResponse
from api.xline.rpc_pb2_grpc import AuthStub
from api.xline.auth_pb2 import UserAddOptions, Permission
from api.xline.rpc_pb2 import (
    AuthEnableRequest,
    AuthEnableResponse,
    AuthDisableRequest,
    AuthDisableResponse,
    AuthStatusRequest,
    AuthStatusResponse,
    AuthenticateRequest,
    AuthenticateResponse,
    AuthUserAddRequest,
    AuthUserAddResponse,
    AuthUserGetRequest,
    AuthUserGetResponse,
    AuthUserListRequest,
    AuthUserListResponse,
    AuthUserDeleteRequest,
    AuthUserDeleteResponse,
    AuthUserChangePasswordRequest,
    AuthUserChangePasswordResponse,
    AuthUserGrantRoleRequest,
    AuthUserGrantRoleResponse,
    AuthUserRevokeRoleRequest,
    AuthUserRevokeRoleResponse,
    AuthRoleAddRequest,
    AuthRoleAddResponse,
    AuthRoleGetRequest,
    AuthRoleGetResponse,
    AuthRoleListRequest,
    AuthRoleListResponse,
    AuthRoleDeleteRequest,
    AuthRoleDeleteResponse,
    AuthRoleGrantPermissionRequest,
    AuthRoleGrantPermissionResponse,
    AuthRoleRevokePermissionRequest,
    AuthRoleRevokePermissionResponse,
)

PermissionType = Permission.Type

PERM_READ = Permission.Type.READ
PERM_WRITE = Permission.Type.WRITE
PERM_READWRITE = Permission.Type.READWRITE


class AuthClient:
    """
    Client for Auth operations.

    Attributes:
        curp_client: The client running the CURP protocol, communicate with all servers.
        auth_client: The auth RPC client, only communicate with one server at a time.
        token: The auth token.
    """

    curp_client: CurpClient
    auth_client: AuthStub
    token: Optional[str]

    def __init__(self, curp_client: CurpClient, channel: grpc.Channel, token: Optional[str]) -> None:
        self.curp_client = curp_client
        self.auth_client = AuthStub(channel=channel)
        self.token = token

    async def auth_enable(self) -> AuthEnableResponse:
        """
        Enables authentication.
        """
        request_with_token = RequestWithToken(token=self.token, auth_enable_request=AuthEnableRequest())
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_enable_response

    async def auth_disable(self) -> AuthDisableResponse:
        """
        Disables authentication.
        """
        request_with_token = RequestWithToken(token=self.token, auth_disable_request=AuthDisableRequest())
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_disable_response

    async def auth_status(self) -> AuthStatusResponse:
        """
        Gets authentication status.
        """
        request_with_token = RequestWithToken(token=self.token, auth_status_request=AuthStatusRequest())
        er = await self.handle_req(req=request_with_token, use_fast_path=True)
        return er.auth_status_response

    async def authenticate(self, name: str, password: str) -> AuthenticateResponse:
        """
        Process an authentication request, and return the auth token.
        """
        res: AuthenticateResponse = await self.auth_client.Authenticate(
            AuthenticateRequest(name=name, password=password)
        )
        return res

    async def user_add(self, name: str, password: str = "", no_password: bool = True) -> AuthUserAddResponse:
        """
        Add an user.
        """
        if name == "":
            msg = "user name is empty"
            raise Exception(msg)
        need_password = not no_password
        if need_password and password == "":
            msg = "Password is required but not provided"
            raise Exception(msg)

        hashed_password = ""
        if need_password:
            hashed_password = self.hash_password(password)
            password = ""

        req = AuthUserAddRequest(
            name=name,
            password=password,
            hashedPassword=hashed_password,
            options=UserAddOptions(no_password=no_password),
        )
        request_with_token = RequestWithToken(token=self.token, auth_user_add_request=req)
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_user_add_response

    async def user_get(self, name: str) -> AuthUserGetResponse:
        """
        Gets the user info by the user name.
        """
        request_with_token = RequestWithToken(token=self.token, auth_user_get_request=AuthUserGetRequest(name=name))
        er = await self.handle_req(req=request_with_token, use_fast_path=True)
        return er.auth_user_get_response

    async def user_list(self) -> AuthUserListResponse:
        """
        Lists all users.
        """
        request_with_token = RequestWithToken(token=self.token, auth_user_list_request=AuthUserListRequest())
        er = await self.handle_req(req=request_with_token, use_fast_path=True)
        return er.auth_user_list_response

    async def user_delete(self, name: str) -> AuthUserDeleteResponse:
        """
        Deletes the user by the user name.
        """
        request_with_token = RequestWithToken(
            token=self.token, auth_user_delete_request=AuthUserDeleteRequest(name=name)
        )
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_user_delete_response

    async def user_change_password(self, name: str, password: str) -> AuthUserChangePasswordResponse:
        """
        Change password for an user.
        """
        if password == "":
            msg = "password is empty"
            raise Exception(msg)

        hashed_password = self.hash_password(password)

        request_with_token = RequestWithToken(
            token=self.token,
            auth_user_change_password_request=AuthUserChangePasswordRequest(name=name, hashedPassword=hashed_password),
        )
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_user_change_password_response

    async def user_grant_role(self, user: str, role: str) -> AuthUserGrantRoleResponse:
        """
        Grant role for a user.
        """
        request_with_token = RequestWithToken(
            token=self.token, auth_user_grant_role_request=AuthUserGrantRoleRequest(user=user, role=role)
        )
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_user_grant_role_response

    async def user_revoke_role(self, name: str, role: str) -> AuthUserRevokeRoleResponse:
        """
        Revoke role for a user.
        """
        request_with_token = RequestWithToken(
            token=self.token, auth_user_revoke_role_request=AuthUserRevokeRoleRequest(name=name, role=role)
        )
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_user_revoke_role_response

    async def role_add(self, name: str) -> AuthRoleAddResponse:
        """
        Adds role.
        """
        if name == "":
            msg = "role name is empty"
            raise Exception(msg)

        request_with_token = RequestWithToken(token=self.token, auth_role_add_request=AuthRoleAddRequest(name=name))
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_role_add_response

    async def role_get(self, role: str) -> AuthRoleGetResponse:
        """
        Gets role.
        """
        request_with_token = RequestWithToken(token=self.token, auth_role_get_request=AuthRoleGetRequest(role=role))
        er = await self.handle_req(req=request_with_token, use_fast_path=True)
        return er.auth_role_get_response

    async def role_list(self) -> AuthRoleListResponse:
        """
        Lists role.
        """
        request_with_token = RequestWithToken(token=self.token, auth_role_list_request=AuthRoleListRequest())
        er = await self.handle_req(req=request_with_token, use_fast_path=True)
        return er.auth_role_list_response

    async def role_delete(self, role: str) -> AuthRoleDeleteResponse:
        """
        Deletes role.
        """
        request_with_token = RequestWithToken(
            token=self.token, auth_role_delete_request=AuthRoleDeleteRequest(role=role)
        )
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_role_delete_response

    async def role_grant_permission(
        self, name: str, perm_type: PermissionType, key: bytes, range_end: bytes | None = None
    ) -> AuthRoleGrantPermissionResponse:
        """
        Grants role permission.
        """
        req = AuthRoleGrantPermissionRequest(
            name=name, perm=Permission(permType=perm_type, key=key, range_end=range_end)
        )
        request_with_token = RequestWithToken(token=self.token, auth_role_grant_permission_request=req)
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_role_grant_permission_response

    async def role_revoke_permission(
        self, role: str, key: bytes, range_end: bytes | None = None
    ) -> AuthRoleRevokePermissionResponse:
        """
        Revoke role permission.
        """
        req = AuthRoleRevokePermissionRequest(role=role, key=key, range_end=range_end)
        request_with_token = RequestWithToken(token=self.token, auth_role_revoke_permission_request=req)
        er = await self.handle_req(req=request_with_token, use_fast_path=False)
        return er.auth_role_revoke_permission_response

    async def handle_req(self, req: RequestWithToken, use_fast_path: bool) -> CommandResponse:
        """
        Send request using fast path or slow path.
        """
        cmd = Command(request=req)

        if use_fast_path:
            er, _ = await self.curp_client.propose(cmd, True)
            return er
        else:
            er, asr = await self.curp_client.propose(cmd, False)
            if asr is None:
                msg = "sync_res is always Some when use_fast_path is false"
                raise Exception(msg)
            return er

    @staticmethod
    def hash_password(password: str) -> str:
        """Generate hash of the password."""
        ROUNDS = 200000
        SALTSIZE = 16
        hashed_password = pbkdf2_sha256.using(rounds=ROUNDS, salt_size=SALTSIZE).hash(password)
        return hashed_password
