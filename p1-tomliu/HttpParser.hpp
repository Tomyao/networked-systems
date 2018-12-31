#ifndef HTTPPARSER_HPP
#define HTTPPARSER_HPP

#include <string>
#include <stdint.h>

using namespace std;

typedef struct ResponseStruct_t {
    // DEFINE YOUR DATA STRUCTURE HERE
    int response_code;
    std::string last_modified;
    std::string content_type;
    int content_length;
    std::string response_body;
    bool should_terminate;
} ResponseStruct;

class HttpParser {
public:
	static ResponseStruct parse(string request, string doc_root);
};

#endif // HTTPPARSER_HPP
