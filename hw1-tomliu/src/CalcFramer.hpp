#ifndef CALCFRAMER_HPP
#define CALCFRAMER_HPP

#include <iostream>

class CalcFramer {
public:
	void append(std::string chars);

	// Does this buffer contain at least one complete message?
	bool hasMessage() const;

	// Returns the first instruction
	std::string topMessage() const;

	// Removes the first instruction
	void popMessage();

	// prints the string to an ostream (useful for debugging)
	void printToStream(std::ostream& stream) const;

protected:
	// PUT ANY FIELDS YOU NEED HERE
    char my_buffer[4096];
    std::string my_message;
};

#endif // CALCFRAMER_HPP
