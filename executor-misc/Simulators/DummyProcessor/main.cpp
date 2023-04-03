#include <iostream>
#include <fstream>
using namespace std;

#include <time.h>

int main(int argc, char *argv[])
{
    cout << "========== DUMMY PROCESSOR called ==============\n";
    cout << "DUMMY PROCESSOR called with a number of " << argc << " arguments\n";
    cout << "The arguments are:\n";
    for(int i = 1; i < argc; i++) {
        cout << '\t' << argv[i] << '\n';
    }
    cout << "========== DUMMY PROCESSOR end ==============\n";

    ofstream myfile;
    time_t ttTime;
    struct tm * timeinfo;
    time(&ttTime);
    timeinfo = localtime (&ttTime);

    char bufTime[100];
    strftime (bufTime,sizeof(bufTime),"%Y-%m-%d %H:%M:%S",timeinfo);
    myfile.open ("../../logs/dummy_processor.txt");
    myfile << bufTime << "\n";
    myfile.close();

    return 0;
}

