#include <assert.h>
#include "HttpFramer.hpp"
#include <iostream>

using namespace std;

void HttpFramer::append(string chars)
{
	// PUT YOUR CODE HERE
    my_requests = my_requests + chars;
}

bool HttpFramer::hasRequest() const
{
	// PUT YOUR CODE HERE
    // if message contains /r/n/r/n return true
    int index = 0;
    while ((index + 3) < static_cast<int>(my_requests.length())) {
        if (my_requests[index] == '\r' and my_requests[index+1] == '\n' and my_requests[index+2] == '\r' and my_requests[index+3] == '\n') {
            return true;
        }
        index++;
    }
	return false;
}

string HttpFramer::topRequest() const
{
	// PUT YOUR CODE HERE
    int index = 0;
    while ((index + 3) < static_cast<int>(my_requests.length())) {
        if (my_requests[index] == '\r' and my_requests[index+1] == '\n' and my_requests[index+2] == '\r' and my_requests[index+3] == '\n') {
            return my_requests.substr(0,(index + 4));
        }
        index++;
    }
	return string();
}

void HttpFramer::popRequest()
{
	// PUT YOUR CODE HERE
    int index = 0;
    while ((index + 3) < static_cast<int>(my_requests.length())) {
        if (my_requests[index] == '\r' and my_requests[index+1] == '\n' and my_requests[index+2] == '\r' and my_requests[index+3] == '\n') {
            my_requests = my_requests.substr(index + 4);
            break;
        }
        index++;
    }
}

void HttpFramer::printToStream(ostream& stream) const
{
	// PUT YOUR CODE HERE
    stream << my_requests;
}
