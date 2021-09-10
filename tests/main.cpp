#include <QCoreApplication>
#include <QTest>

#include "testqstring.hpp"
#include "serialization.hpp"
#include "schedulertests.h"
#include "producthandlertests.h"

int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    Q_UNUSED(app);

    int r = 0;

    ProductHandlerTests tcPrdHandlers;
    r |= QTest::qExec(&tcPrdHandlers, argc, argv);


//    Serialization tcSer;
//    r |= QTest::qExec(&tcSer, argc, argv);

//    SchedulerTests tcSch;
//    r |= QTest::qExec(&tcSch, argc, argv);

    return r ? 1 : 0;
}
