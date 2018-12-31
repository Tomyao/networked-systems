#include <assert.h>
#include "HttpParser.hpp"
#include <sstream>
#include <iostream>
#include <sys/stat.h>
#include <fstream>
#include <vector>
#include <time.h>

ResponseStruct HttpParser::parse(std::string request, string doc_root)
{
    ResponseStruct ret;
    ret.should_terminate = false;
    
    // strings to store request
    std::string my_method;
    std::string my_url;
    std::string highest_http;
    
    // bool to check for a Host field
    bool host_found = false;
    
    // bool for whether there is a malformed request
    bool client_error = false;
    
    // for keeping track of lines
    std::string my_line;
    int start_index = 0;
    int index = 0;
    
    // extract the request while checking for basic formatting errors
    while ((index + 1) < static_cast<int>(request.length())) {
        if (request[index] == '\r' and request[index + 1] == '\n') {
            // construct line
            my_line = request.substr(start_index,(index - start_index + 2));
            if (start_index == 0) { // first line should be GET the_url HTTP/1.1
                // split line into method, url, and highest http supported
                std::istringstream my_stream(my_line);
                getline(my_stream, my_method, ' ');
                // check for failbit
                if (my_stream.fail() == 1) {
                    cerr << "Missing first argument GET" << "\n";
                    client_error = true;
                }
                getline(my_stream, my_url, ' ');
                // check for failbit
                if (my_stream.fail() == 1) {
                    cerr << "Missing second argument url" << "\n";
                    client_error = true;
                }
                getline(my_stream, highest_http, ' ');
                // check for failbit
                if (my_stream.fail() == 1) {
                    cerr << "Missing third argument highest html supported" << "\n";
                    client_error = true;
                }
                // check for presence of eofbit
                if (my_stream.eof() != 1) {
                    cerr << "Too many arguments in first line" << "\n";
                    client_error = true;
                }
            } else if ((index + 2) == static_cast<int>(request.length())) { // end of input
                cout << "Finished checking basic formatting" << "\n";
                break;
            } else { // key value pairs
                // split line into two parts
                std::istringstream my_stream(my_line);
                std::string part1;
                std::string part2;
                getline(my_stream, part1, ':');
                // check for failbit
                if (my_stream.fail() == 1) {
                    cerr << "Missing first argument in key value pair" << "\n";
                    client_error = true;
                }
                getline(my_stream, part2); // extract everything after first colon
                // check for failbit
                if (my_stream.fail() == 1) {
                    cerr << "Missing second argument in key value pair" << "\n";
                    client_error = true;
                } else {
                    // remove \r from end of part2 (for some reason, using getline without a delimiter extracts only \r, while using a delimiter extracts \r\n)
                    part2 = part2.substr(0,part2.length()-1);
                    // remove white space from beginning and end
                    int strStart = part2.find_first_not_of(' ');
                    int strEnd = part2.find_last_not_of(' ');
                    if (strStart == strEnd) {
                        part2 = "";
                    } else {
                        part2 = part2.substr(strStart, strEnd - strStart + 1);
                    }
                    // check that part2 isn't empty after removing white space
                    if (part2 == "") {
                        cerr << "Second argument is blank in key value pair" << "\n";
                        client_error = true;
                    }
                }
                // check for a Host field
                if (part1 == "Host") {
                    cout << "Host: " << part2 << "\n";
                    host_found = true;
                }
                // check for Connection: close
                if (part1 == "Connection" and part2 == "close") {
                    // set termination to true
                    cout << "Termination requested" << "\n";
                    ret.should_terminate = true;
                }
            }
            // increment start_index for next line
            start_index = index + 2;
            //std::cout << my_line;
        }
        index++;
    }
    
    // check that a host was found
    if (!host_found) {
        cerr << "No host field found in request" << "\n";
        client_error = true;
    }
    
    // check for client error on basic formatting
    if (client_error) {
        ret.response_code = 400;
        return ret;
    }
    
    // check method is GET
    if (my_method != "GET") {
        cerr << "Method is not GET" << "\n";
        client_error = true;
    }
    // if url is just /, change it into /index.html
    if (my_url == "/") {
        my_url = "/index.html";
    }
    // check highest_http is HTTP/1.1
    if (highest_http != "HTTP/1.1\r\n") {
        cerr << "Highest http is not precisely HTTP/1.1" << "\n";
        client_error = true;
    }
    
    // check for client error again on more specific formatting
    if (client_error) {
        ret.response_code = 400;
        return ret;
    }
    
    // check that url does not escape directory
    if (doc_root.back() == '/') {
        doc_root = doc_root.substr(0,doc_root.length()-1); // strip / from end of doc_root if it exists
    }
    // get real path
    string my_path = doc_root + my_url;
    //char temp_realpath[PATH_MAX]; //no PATH_MAX on ieng6
    char temp_realpath[4096];
    realpath(my_path.c_str(), temp_realpath);
    string my_realpath(temp_realpath);
    cout << "Resource requested: " << my_realpath << "\n";
    // check real path is at least as long as doc_root
    if (my_realpath.length() < doc_root.length()) {
        cerr << "Client tried to escape document root (real path too short)" << "\n";
        ret.response_code = 404;
        return ret;
    }
    // check real path starts with doc_root
    if (my_realpath.substr(0,doc_root.length()) != doc_root) {
        cerr << "Client tried to escape document root (real path does not start with doc_root)" << "\n";
        ret.response_code = 404;
        return ret;
    }
    
    // check that requested resource exists
    struct stat temp_buffer;
    if (stat(my_realpath.c_str(), &temp_buffer) != 0) {
        cerr << "Requested resource does not exist" << "\n";
        ret.response_code = 404;
        return ret;
    }
    
    // check that requested resource is world readable
    if (!(temp_buffer.st_mode & S_IROTH)) {
        cerr << "Requested resource is not world readable" << "\n";
        ret.response_code = 403;
        return ret;
    }
    
    // check the requested resource is supported (html, jpg, or png)
    if (my_realpath.length() < 4) { // check path is at least 4 chars long
        cerr << "Path too short to be html, jpg, or png" << "\n";
        ret.response_code = 400;
        return ret;
    }
    // check url is html, jpg, or png
    if (my_realpath.substr(my_realpath.length() - 5) == ".html") {
        ret.content_type = "text/html";
    } else if (my_realpath.substr(my_realpath.length() - 4) == ".jpg") {
        ret.content_type = "image/jpeg";
    } else if (my_realpath.substr(my_realpath.length() - 4) == ".png") {
        ret.content_type = "image/png";
    } else {
        cerr << "Requested resource is not html, jpg, or png" << "\n";
        ret.response_code = 400;
        return ret;
    }
    
    // get the last modified time
    time_t my_time = temp_buffer.st_mtime;
    struct tm local_time;
    localtime_r(&my_time, &local_time);
    char time_buffer[1024];
    strftime(time_buffer, sizeof(time_buffer), "%a, %d %b %G %X GMT", &local_time);
    ret.last_modified = time_buffer;
    cout << "Content last modified at: " << ret.last_modified << "\n";
    
    // get the file size
    ret.content_length = temp_buffer.st_size;
    cout << "Content length is: " << ret.content_length << "\n";
    
    // process requested resource
    ifstream my_filestream;
    if (ret.content_type == "text/html") { // read normally for html file
        cout << "Opening html file" << "\n";
        my_filestream.open(my_realpath);
    } else { // read in binary for image files
        cout << "Opening image file in binary" << "\n";
        my_filestream.open(my_realpath, std::ios::in | std::ios::binary);
    }
    std::vector<char> content_buffer((std::istreambuf_iterator<char>(my_filestream)), (std::istreambuf_iterator<char>()));
    std::string mycontent(content_buffer.begin(), content_buffer.end());
    ret.response_body = mycontent;
    
    // set the 200 OK response code
    ret.response_code = 200;
    
	return ret;
}
