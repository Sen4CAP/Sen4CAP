include(../common.pri)

QT += core testlib dbus
QT -= gui

TARGET = tests

DESTDIR = bin

CONFIG -= app_bundle

TEMPLATE = app

INCLUDEPATH += ../Optional

SOURCES += main.cpp \
    testqstring.cpp \
    serialization.cpp \
    reflector.cpp \
    serialization-ops.cpp \
    schedulertests.cpp \
    testscheduledtaskloader.cpp \
    testorcherstratorproxy.cpp \
    producthandlertests.cpp

# cannot link to scheduler app, the files will be included in this project
SOURCES += ../sen2agri-scheduler/taskloader.cpp \
    ../sen2agri-scheduler/schedulerapp.cpp \
    ../sen2agri-scheduler/taskplanner.cpp \
    ../sen2agri-scheduler/orchestratorproxy.cpp \
    ../sen2agri-scheduler/resourcereader.cpp \
    ../sen2agri-scheduler/runestimator.cpp \
    ../sen2agri-orchestrator/processor/products/l2aproducthelper.cpp \
    ../sen2agri-orchestrator/processor/products/generichighlevelproducthelper.cpp \
    ../sen2agri-orchestrator/processor/products/l3bproducthelper.cpp \
    ../sen2agri-orchestrator/processor/products/s1l2producthelper.cpp \
    ../sen2agri-orchestrator/processor/products/productdetails.cpp \
    ../sen2agri-orchestrator/processor/products/producthelper.cpp \
    ../sen2agri-orchestrator/processor/products/producthelperfactory.cpp \
    ../sen2agri-orchestrator/processor/products/maskedl2aproducthelper.cpp

LIBS += -L$$OUT_PWD/../sen2agri-common/ -lsen2agri-common

INCLUDEPATH += $$PWD/../sen2agri-common
INCLUDEPATH += $$PWD/../sen2agri-scheduler
INCLUDEPATH += $$PWD/../sen2agri-orchestrator
DEPENDPATH += $$PWD/../sen2agri-common
DEPENDPATH += $$PWD/../sen2agri-scheduler
DEPENDPATH += $$PWD/../sen2agri-orchestrator

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-common/libsen2agri-common.a

adaptor.files = reflector.xml
adaptor.header_flags = -i ../sen2agri-common/model.hpp

DBUS_ADAPTORS += adaptor

interface.files = reflector.xml
interface.header_flags = -i ../sen2agri-common/model.hpp

DBUS_INTERFACES += interface

orchestrator_interface.files = ../dbus-interfaces/org.esa.sen2agri.orchestrator.xml
orchestrator_interface.header_flags = -i ../sen2agri-common/model.hpp

DBUS_INTERFACES += orchestrator_interface

HEADERS += \
    testqstring.hpp \
    serialization.hpp \
    reflector.hpp \
    serialization-ops.hpp \
    pch.hpp \
    schedulertests.h \
    testscheduletaskloader.hpp \
    testorcherstratorproxy.h \
    producthandlertests.h

HEADERS += \
    ../sen2agri-scheduler/scheduledtask.hpp \
    ../sen2agri-scheduler/taskloader.hpp \
    ../sen2agri-scheduler/schedulerapp.hpp \
    ../sen2agri-scheduler/taskplanner.hpp \
    ../sen2agri-scheduler/resourcereader.hpp \
    ../sen2agri-scheduler/orchestratorproxy.hpp \
    ../sen2agri-scheduler/runestimator.hpp \
    ../sen2agri-orchestrator/processor/products/l2aproducthelper.h \
    ../sen2agri-orchestrator/processor/products/generichighlevelproducthelper.h \
    ../sen2agri-orchestrator/processor/products/l3bproducthelper.h \
    ../sen2agri-orchestrator/processor/products/s1l2producthelper.h \
    ../sen2agri-orchestrator/processor/products/producthelper.h \
    ../sen2agri-orchestrator/processor/products/producthelperfactory.h \
    ../sen2agri-orchestrator/processor/products/maskedl2aproducthelper.h


DISTFILES += \
    reflector.xml
