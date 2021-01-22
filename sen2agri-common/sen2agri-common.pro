include(../common.pri)

QT -= gui
QT += dbus

TARGET = sen2agri-common
TEMPLATE = lib

CONFIG += staticlib

INCLUDEPATH += ../Optional

SOURCES += \
    model.cpp \
    logger.cpp \
    stopwatch.cpp \
    dbus_future_utils.cpp \
    json_conversions.cpp \
    configuration.cpp \
    json_utils.cpp

HEADERS += \
    model.hpp \
    pch.hpp \
    logger.hpp \
    make_unique.hpp \
    optional_util.hpp \
    type_traits_ext.hpp \
    stopwatch.hpp \
    dbus_future_utils.hpp \
    json_conversions.hpp \
    configuration.hpp \
    json_utils.hpp
