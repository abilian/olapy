from __future__ import absolute_import, division, print_function

from spyne import ComplexModel, Integer, String, Unicode, XmlAttribute


class Tuple(object):

    def __init__(self, Hierarchy, UName, Caption, LName, LNum, DisplayInfo,
                 PARENT_UNIQUE_NAME, HIERARCHY_UNIQUE_NAME, Value):
        self.Hierarchy = Hierarchy
        self.UName = UName
        self.Caption = Caption
        self.LName = LName
        self.LNum = LNum
        self.DisplayInfo = DisplayInfo
        self.PARENT_UNIQUE_NAME = PARENT_UNIQUE_NAME
        self.HIERARCHY_UNIQUE_NAME = HIERARCHY_UNIQUE_NAME
        self.Value = Value

    def __str__(self):
        return """
        Hierarchy : {0}
        UName : {1}
        Caption : {2}
        LName : {3}
        LNum : {4}
        DisplayInfo : {5}
        PARENT_UNIQUE_NAME : {6}
        HIERARCHY_UNIQUE_NAME : {7}
        Value : {8}
        """.format(self.Hierarchy, self.UName, self.Caption, self.LName,
                   self.LNum, self.DisplayInfo, self.PARENT_UNIQUE_NAME,
                   self.HIERARCHY_UNIQUE_NAME, self.Value)


class Property(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis"
    _type_info = {
        'LocaleIdentifier': Unicode,
        'Format': Unicode,
        'Catalog': Unicode,
        'Content': Unicode,
        'DataSourceInfo': Unicode,
        'Password': Unicode,
        'StateSupport': Unicode,
        'Timeout': Unicode,
        'ProviderVersion': Unicode,
        'BASE_CUBE_NAME': Unicode,
        'AxisFormat': Unicode,
        'BeginRange': Unicode,
        'EndRange': Unicode,
        'MDXSupport': Unicode,
        'ProviderName': Unicode,
        'UserName': Unicode
    }


class Restriction(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis"
    _type_info = {
        'CATALOG_NAME': Unicode,
        'SCHEMA_NAME': Unicode,
        'CUBE_NAME': Unicode,
        'MEMBER_UNIQUE_NAME': Unicode,
        'DIMENSION_UNIQUE_NAME': Unicode,
        'HIERARCHY_UNIQUE_NAME': Unicode,
        'LEVEL_UNIQUE_NAME': Unicode,
        'TREE_OP': Integer,
        'PropertyName': Unicode,
        'SchemaName': Unicode,
        'HIERARCHY_VISIBILITY': Integer,
        'MEASURE_VISIBILITY': Integer,
        'PROPERTY_TYPE': Integer
    }


class Session(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis"
    SessionId = XmlAttribute(Unicode)


class Restrictionlist(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis"
    __type_name__ = "Restrictions"
    RestrictionList = Restriction


class Propertielist(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis"
    __type_name__ = "Properties"
    PropertyList = Property


class Command(ComplexModel):
    _type_info = {'Statement': Unicode, }


class ExecuteRequest(ComplexModel):
    Command = Command
    Properties = Propertielist


class DiscoverRequest(ComplexModel):
    RequestType = Unicode
    Restrictions = Restrictionlist
    Properties = Propertielist


class DiscoverResponse(ComplexModel):
    __namespace__ = "urn:schemas-microsoft-com:xml-analysis:rowset"
    root = String
