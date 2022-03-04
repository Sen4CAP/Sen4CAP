include(../common.pri)

QT -= gui
QT += core dbus sql network

TARGET = sen2agri-orchestrator

DESTDIR = bin

CONFIG -= app_bundle

INCLUDEPATH += ../Optional

TEMPLATE = app

adaptor.files = ../dbus-interfaces/org.esa.sen2agri.orchestrator.xml
adaptor.header_flags = -i ../sen2agri-common/model.hpp

DBUS_ADAPTORS += adaptor

processors_executor_interface.files = ../dbus-interfaces/org.esa.sen2agri.processorsExecutor.xml
processors_executor_interface.header_flags = -i ../sen2agri-common/model.hpp

DBUS_INTERFACES += processors_executor_interface

SOURCES += main.cpp \
    orchestrator.cpp \
    orchestratorworker.cpp \
    eventprocessingcontext.cpp \
    executioncontextbase.cpp \
    processorhandler.cpp \
    processor/croptypehandler.cpp \
    processor/cropmaskhandler.cpp \
    tasktosubmit.cpp \
    processor/compositehandler.cpp \
    processor/lairetrievalhandler.cpp \
    processor/lairetrievalhandler_l3b.cpp \
    processor/lairetrhandler_multidt_base.cpp \
    processor/lairetrievalhandler_l3c.cpp \
    processor/lairetrievalhandler_l3d.cpp \
    processor/maccshdrmeananglesreader.cpp \
    processor/phenondvihandler.cpp \
    processorhandlerhelper.cpp \
    schedulingcontext.cpp \
    processor/ndvihandler.cpp \
    processor/lairetrievalhandler_l3b_new.cpp \
    processor/s4c_croptypehandler.cpp \
    processor/agricpracticeshandler.cpp \
    processor/grasslandmowinghandler.cpp \
    processor/s4c_utils.cpp \
    processor/s4c_markersdb1.cpp \
    processor/s4c_mdb1_dataextract_steps_builder.cpp \
    http/controller/orchestratorcontroller.cpp \
    adaptor/dbusorchestratoradaptor.cpp \
    adaptor/httporchestratoradaptor.cpp \
    executorclient/executorproxy.cpp \
    executorclient/dbusexecutorproxy.cpp \
    executorclient/httpexecutorproxy.cpp \
    executorclient/executorproxyfactory.cpp \
    stepexecutiondecorator.cpp \
    processor/products/producthelper.cpp \
    processor/products/producthelperfactory.cpp \
    processor/products/s1l2producthelper.cpp \
    processor/products/l2aproducthelper.cpp \
    processor/products/generichighlevelproducthelper.cpp \
    processor/products/maskedl2aproducthelper.cpp \
    processor/products/tilestimeseries.cpp \
    processor/masked_l2a_handler.cpp \
    processor/s4s_permanent_crop_handler.cpp \
    processor/products/productdetails.cpp \
    productdetailsbuilder.cpp \
    processor/s4s_yieldhandler.cpp \
    processor/trex_handler.cpp \
    processor/products/l3bproducthelper.cpp \
    processor/genericcompositehandlerbase.cpp \
    processor/compositehandlerindicators.cpp \
    processor/compositehandlers1.cpp

HEADERS += \
    pch.hpp \
    orchestrator.hpp \
    orchestratorworker.hpp \
    executioncontextbase.hpp \
    eventprocessingcontext.hpp \
    processorhandler.hpp \
    processor/croptypehandler.hpp \
    processor/cropmaskhandler.hpp \
    tasktosubmit.hpp \
    processor/compositehandler.hpp \
    processor/lairetrievalhandler.hpp \
    processor/lairetrievalhandler_l3b.hpp \
    processor/lairetrhandler_multidt_base.hpp \
    processor/lairetrievalhandler_l3c.hpp \
    processor/lairetrievalhandler_l3d.hpp \
    processor/maccshdrmeananglesreader.hpp \
    processor/phenondvihandler.hpp \
    processorhandlerhelper.h \
    schedulingcontext.h \
    processor/ndvihandler.hpp \
    processor/lairetrievalhandler_l3b_new.hpp \
    processor/s4c_croptypehandler.hpp \
    processor/agricpracticeshandler.hpp \
    processor/grasslandmowinghandler.hpp \
    processor/s4c_utils.hpp \
    processor/s4c_markersdb1.hpp \
    processor/s4c_mdb1_dataextract_steps_builder.hpp \
    http/controller/orchestratorcontroller.hpp \
    adaptor/dbusorchestratoradaptor.h \
    adaptor/httporchestratoradaptor.h \
    executorclient/dbusexecutorproxy.hpp \
    executorclient/httpexecutorproxy.hpp \
    executorclient/executorproxy.hpp \
    executorclient/executorproxyfactory.h \
    stepexecutiondecorator.h \
    processor/products/producthelper.h \
    processor/products/producthelperfactory.h \
    processor/products/s1l2producthelper.h \
    processor/products/l2aproducthelper.h \
    processor/products/generichighlevelproducthelper.h \
    processor/products/maskedl2aproducthelper.h \
    processor/products/tilestimeseries.hpp \
    processor/masked_l2a_handler.hpp \
    processor/s4s_permanent_crop_handler.hpp \
    processor/products/productdetails.h \
    productdetailsbuilder.h \
    processor/s4s_yieldhandler.hpp \
    processor/trex_handler.hpp \
    processor/products/l3bproducthelper.h \
    processor/compositehandlers1.hpp \
    processor/compositehandlerindicators.hpp \
    processor/genericcompositehandlerbase.hpp

DISTFILES += \
    ../dbus-interfaces/org.esa.sen2agri.orchestrator.xml \
    dist/org.esa.sen2agri.orchestrator.conf \
    dist/org.esa.sen2agri.orchestrator.service \
    dist/sen2agri-orchestrator.service

target.path = /usr/bin

interface.path = /usr/share/dbus-1/interfaces
interface.files = ../dbus-interfaces/org.esa.sen2agri.orchestrator.xml

dbus-policy.path = /etc/dbus-1/system.d
dbus-policy.files = dist/org.esa.sen2agri.orchestrator.conf

dbus-service.path = /usr/share/dbus-1/system-services
dbus-service.files = dist/org.esa.sen2agri.orchestrator.service

systemd-service.path = /usr/lib/systemd/system
systemd-service.files = dist/sen2agri-orchestrator.service

INSTALLS += target interface dbus-policy dbus-service systemd-service

LIBS += -L$$OUT_PWD/../sen2agri-persistence/ -lsen2agri-persistence

INCLUDEPATH += $$PWD/../sen2agri-persistence
DEPENDPATH += $$PWD/../sen2agri-persistence

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-persistence/libsen2agri-persistence.a

LIBS += -L$$OUT_PWD/../sen2agri-common/ -lsen2agri-common

INCLUDEPATH += $$PWD/../sen2agri-common
DEPENDPATH += $$PWD/../sen2agri-common

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-common/libsen2agri-common.a

QTWEBAPP = -lQtWebApp
CONFIG(debug, debug|release) {
    QTWEBAPP = $$join(QTWEBAPP,,,d)
}

LIBS += -L$$OUT_PWD/../QtWebApp/ $$QTWEBAPP

INCLUDEPATH += $$PWD/../QtWebApp
DEPENDPATH += $$PWD/../QtWebApp

CONFIG(debug, debug|release) {
    LIBQTWEBAPP = $$OUT_PWD/../QtWebApp/libQtWebAppd.so
}
CONFIG(release, debug|release) {
    LIBQTWEBAPP = $$OUT_PWD/../QtWebApp/libQtWebApp.so
}

PRE_TARGETDEPS += $$LIBQTWEBAPP


LIBS += -L$$OUT_PWD/../sen2agri-http-server-common/ -lsen2agri-http-server-common

INCLUDEPATH += $$PWD/../sen2agri-http-server-common
DEPENDPATH += $$PWD/../sen2agri-http-server-common

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-http-server-common/libsen2agri-http-server-common.a
