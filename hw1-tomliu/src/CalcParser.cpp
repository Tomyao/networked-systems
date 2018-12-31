#include <assert.h>
#include "CalcParser.hpp"
#include <sstream>
#include <iostream>

CalcInstruction CalcParser::parse(std::string insstr, uint64_t currval)
{
	CalcInstruction ret;

	// PUT YOUR CODE HERE
    uint64_t returnval = currval;
    
    // read line by line
    std::string my_line;
    int start_index = 0;
    int index = 0;
    
    while (index < insstr.length()) {
        if (insstr[index] == '\n') {
            // construct line
            my_line = insstr.substr(start_index,(index - start_index + 1));
            // set new start index for next line
            start_index = index + 1;
            // split line into two parts
            std::istringstream my_stream(my_line);
            std::string my_string;
            std::string my_string2;
            uint64_t my_int;
            getline(my_stream, my_string, ' ');
            getline(my_stream, my_string2, ' ');
            std::istringstream tempstream(my_string2);
            tempstream >> my_int;
            // do calcs
            if (my_string == "SET") {
                returnval = my_int;
            }
            if (my_string == "ADD") {
                returnval += my_int;
            }
            if (my_string == "SUB") {
                returnval -= my_int;
            }
        }
        index++;
    }
    
    ret.value = returnval;
    ret.should_return = false;
    if (my_line == "\r\n") {
        ret.should_return = true;
    }

	return ret;
}
