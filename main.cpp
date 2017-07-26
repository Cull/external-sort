#include<stdio.h>
#include<fstream>
#include<sys/stat.h>
#include<stdint.h>
#include<boost/chrono/chrono.hpp>
#include<boost/timer/timer.hpp>

#include"runctrl.h"

int main (int argc, char *argv[])
{
    boost::timer::cpu_timer timer;
    RunCtrl controller(320);
    controller.run_sort();
    typedef boost::chrono::duration<double> sec;
    sec seconds = boost::chrono::nanoseconds(timer.elapsed().user);
    std::cout << seconds.count() << std::endl;
    controller.test_result();
    return 0;
}
