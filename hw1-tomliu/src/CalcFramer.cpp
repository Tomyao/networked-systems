#include <assert.h>
#include "CalcFramer.hpp"
#include <iostream>

using namespace std;

void CalcFramer::append(string chars)
{
	// PUT YOUR CODE HERE
    my_message = my_message + chars;
}

bool CalcFramer::hasMessage() const
{
	// PUT YOUR CODE HERE
    // if message contains /r/n return true
    int index = 0;
    while ((index + 1) < my_message.length()) {
        if (my_message[index] == '\r' and my_message[index+1] == '\n') {
            return true;
        }
        index++;
    }
	return false;
}

string CalcFramer::topMessage() const
{
	// PUT YOUR CODE HERE
    int index = 0;
    while ((index + 1) < my_message.length()) {
        if ((my_message[index] == '\r') and (my_message[index+1] == '\n')) {
            return my_message.substr(0,(index + 2));
        }
        index++;
    }
	return string();
}

void CalcFramer::popMessage()
{
	// PUT YOUR CODE HERE
    int index = 0;
    while ((index + 1) < my_message.length()) {
        if ((my_message[index] == '\r') and (my_message[index+1] == '\n')) {
            my_message = my_message.substr(index + 2);
            break;
        }
        index++;
    }
}

void CalcFramer::printToStream(ostream& stream) const
{
	// PUT YOUR CODE HERE
    stream << my_message;
}
