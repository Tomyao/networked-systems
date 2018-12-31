#ifndef HTTPFRAMER_HPP
#define HTTPFRAMER_HPP

#include <iostream>

class HttpFramer {
public:
	void append(std::string chars);

	// Does this buffer contain at least one complete request?
	bool hasRequest() const;

	// Returns the first request
	std::string topRequest() const;

	// Removes the first request
	void popRequest();

	// prints the string to an ostream (useful for debugging)
	void printToStream(std::ostream& stream) const;

protected:
	// PUT ANY FIELDS YOU NEED HERE
    std::string my_requests;
};

#endif // HTTPFRAMER_HPP
