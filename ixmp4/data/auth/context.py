import re

import pandas as pd

from ixmp4 import db
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo
from ixmp4.conf.user import User
from ixmp4.data.db import Model
from ixmp4.db import utils


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
        df = df.dropna()

        def convert_to_regex(m: str) -> str:
            return re.escape(m).replace("\\*", ".*")

        def convert_to_like(m: str) -> str:
            return m.replace("*", "%").replace("_", "[_]")

        df["regex"] = df["model"].apply(convert_to_regex)
        df["like"] = df["model"].apply(convert_to_like)
        return df

    def apply(self, access_type: str, exc: db.sql.Select) -> db.sql.Select:
        if self.is_managed:
            return exc
        if self.user.is_superuser:
            return exc

        if utils.is_joined(exc, Model):
            perms = self.tabulate_permissions()
            if perms.empty:
                return exc.where(False)  # type: ignore
            if access_type == "edit":
                perms = perms.where(perms["access_type"] == "EDIT").dropna()
            # `*` is used as wildcard in permission logic, replaced by sql-wildcard `%`
            conditions = [Model.name.like(p["like"]) for i, p in perms.iterrows()]
            exc = exc.where(db.or_(*conditions))
        return exc

    def check_access(self, access_type: str, model_name: str) -> bool:
        if self.is_managed:
            return True
        if self.user.is_superuser:
            return True

        perms = self.tabulate_permissions()
        if perms.empty:
            return False
        if access_type == "edit":
            perms = perms.where(perms["access_type"] == "EDIT").dropna()

        regex = "^" + "|".join(perms["regex"]) + "$"
        match = re.match(regex, model_name)
        return match is not None

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
