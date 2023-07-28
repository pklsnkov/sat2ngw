# -*- coding: utf-8 -*-

#******************************************************************************
#
# NGSatSearch
# ---------------------------------------------------------
# Module for searching and downloading satellite data
#
# Copyright (C) 2019 NextGIS (info@nextgis.org)
#
#******************************************************************************

class UnsupportedPlatform(Exception):
    pass

class InvalidIdentifier(Exception):
    pass

class DatasetNotFound(Exception):
    pass

class InvalidOption(Exception):
    pass

class InvalidMetadata(Exception):
    pass

class AuthorizationError(Exception):
    pass

class QueryError(Exception):
    pass

class InvalidPolygon(Exception):
    pass

class ConnectionError(Exception):
    pass

class ServiceIsNotResponsible(Exception):
    pass