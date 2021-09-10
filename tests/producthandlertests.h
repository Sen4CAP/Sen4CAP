#ifndef PRODUCTHANDLERTESTS_H
#define PRODUCTHANDLERTESTS_H

#include <QObject>

class ProductHandlerTests : public QObject
{
    Q_OBJECT
public:
    explicit ProductHandlerTests(QObject *parent = 0);

private slots:
    void initTestCase();
    void cleanupTestCase();
    void testL2AProductHandler();
    void testS1L2ProductHandler();
    void testGenericHighLevelProductHandler();

};

#endif // PRODUCTHANDLERTESTS_H
