#!/usr/bin/env python

from sqlalchemy import TEXT, VARCHAR, Column

from . import HummingbotBase


class Metadata(HummingbotBase):
    __tablename__ = "Metadata"

    key = Column(VARCHAR(190), primary_key=True, nullable=False)
    value = Column(TEXT, nullable=False)

    def __repr__(self) -> str:
        return f"Metadata(key='{self.key}', value='{self.value}')"
