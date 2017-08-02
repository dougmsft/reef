#include <FluidCoreInterface.h>

int main()
{
    fluid::Initialize(".\\");
    fluid::Connect("1.2.3.4", 20);
    std::vector<unsigned char> test = { 1,2,3,4,5 };
    auto rTaskid = fluid::SubmitRTask(test);
    auto juliaTaskid = fluid::SubmitJuliaTask(test);
    fluid::Shutdown();

    return 0;
}
