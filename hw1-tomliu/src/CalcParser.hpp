#ifndef CALCPARSER_HPP
#define CALCPARSER_HPP

#include <string>
#include <stdint.h>

using namespace std;

typedef struct CalcInstruction_t {
	// DEFINE YOUR DATA STRUCTURE HERE
    uint64_t value;
    bool should_return;
} CalcInstruction;

/*
 * Alternatively:
 * class CalcInstruction {
 *   // DEFINE YOUR CLASS HERE
 * };
 *
 */


class CalcParser {
public:
	static CalcInstruction parse(string insstr, uint64_t currval);
};

#endif // CALCPARSER_HPP
