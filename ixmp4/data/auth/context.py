import pandas as pd

from ixmp4 import db
from ixmp4.db import utils
from ixmp4.data.db import Model

from ixmp4.conf.user import User
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo


class AuthorizationContext(object):
    def __init__(
        self, user: User, manager: ManagerConfig, platform: ManagerPlatformInfo
    ):
        self.user = user
        self.manager = manager
        self.platform = platform

    def tabulate_permissions(self):
        df = self.manager.fetch_user_permissions(
            self.user, self.platform, jti=self.user.jti
        )
        if self.platform.accessibility != ManagerPlatformInfo.Accessibilty.PRIVATE:
            group_df = self.manager.fetch_group_permissions(
                self.platform.access_group, self.platform, jti=self.user.jti
            )
            df = pd.concat([df, group_df])
        return df.dropna()

    def apply(self, access_type: str, exc: db.sql.Select) -> db.sql.Select:
        if utils.is_joined(exc, Model):
            perms = self.tabulate_permissions()
            if perms.empty:
                return exc.where(False)  # type: ignore
            if access_type == "edit":
                perms = perms.where(perms["access_type"] == "EDIT").dropna()
            # `*` is used as wildcard in permission logic, replaced by sql-wildcard `%`
            conditions = [
                Model.name.like(p["model"].replace("*", "%"))
                for i, p in perms.iterrows()
            ]
            exc = exc.where(db.or_(*conditions))
        return exc

    @property
    def is_accessible(self) -> bool:
        if self.user.is_superuser:
            return True
        if self.is_managed:
            return True
        elif self.platform.accessibility == ManagerPlatformInfo.Accessibilty.PUBLIC:
            return True
        elif self.platform.accessibility == ManagerPlatformInfo.Accessibilty.GATED:
            return self.user.is_authenticated

        return self.platform.access_group in self.user.groups

    @property
    def is_viewable(self) -> bool:
        if self.user.is_superuser:
            return True
        if self.is_managed:
            return True

        df = self.tabulate_permissions()
        return not df[
            (df["access_type"] == "EDIT") | (df["access_type"] == "VIEW")
        ].empty

    @property
    def is_editable(self) -> bool:
        if self.user.is_superuser:
            return True
        if self.is_managed:
            return True

        df = self.tabulate_permissions()
        return not df[df["access_type"] == "EDIT"].empty

    @property
    def is_managed(self) -> bool:
        if self.user.is_superuser:
            return True
        return self.platform.management_group in self.user.groups
