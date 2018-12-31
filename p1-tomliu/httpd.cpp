#include <iostream>
#include "httpd.h"
#include <stdio.h>      /* for printf() and fprintf() */
#include <sys/socket.h> /* for recv() and send() */
#include <unistd.h>     /* for close() */
#include <iostream>
#include <assert.h>
#include "HttpFramer.hpp"
#include "HttpParser.hpp"
#include <sstream>
#include <cstring>

using namespace std;

void start_httpd(int clntSocket, string doc_root)
{
	cout << "Connection info (clntSocket: " << clntSocket << ", doc_root: " << doc_root << ")" << endl;
    
    HttpFramer myHttpFramer;
    HttpParser myHttpParser;
    char my_buffer[4096];
    
    for (;;) {
        // get input from recv
        int numBytesRcvd = recv(clntSocket, my_buffer, sizeof(my_buffer), 0);
        if (numBytesRcvd == 0) {
            std::cout << "No response" << "\n";
            break;
        }
        // timeout check
        if (numBytesRcvd < 0 && errno == EWOULDBLOCK) {
            std::cout << "Closing socket due to timeout" << "\n";
            close(clntSocket);
            return;
        }
        
        std::string new_input(my_buffer, numBytesRcvd);
        
        // pass input to framer
        myHttpFramer.append(new_input);
        
        while (myHttpFramer.hasRequest()) {
            std::string new_request = myHttpFramer.topRequest();
            myHttpFramer.popRequest();
            // pass message to parser
            ResponseStruct my_result = myHttpParser.parse(new_request, doc_root);
            // construct message from result of parser
            std::string response_message = "HTTP/1.1 ";
            if (my_result.response_code == 200) {
                response_message += "200 OK\r\n";
                response_message += "Server: httpd\r\n";
                response_message += ("Last-Modified: " + my_result.last_modified + "\r\n");
                response_message += ("Content-Type: " + my_result.content_type + "\r\n");
                response_message += ("Content-Length: " + std::to_string(my_result.content_length) + "\r\n");
                response_message += ("\r\n");
                response_message += (my_result.response_body);
            } else if (my_result.response_code == 400) {
                response_message += "400 Client Error\r\n";
                response_message += "Server: httpd\r\n";
                response_message += "\r\n";
            } else if (my_result.response_code == 403) {
                response_message += "403 Forbidden\r\n";
                response_message += "Server: httpd\r\n";
                response_message += "\r\n";
            } else if (my_result.response_code == 404) {
                response_message += "404 Not Found\r\n";
                response_message += "Server: httpd\r\n";
                response_message += "\r\n";
            } else {
                std::cerr << "Parser did not return a valid response code" << "\n";
            }
            // return message to client
            std::cout << "Sending results of request" << "\n";
            int my_strlen = response_message.length();
            send(clntSocket, response_message.c_str(), my_strlen, 0);
            // if parser found the termination request
            if (my_result.should_terminate) {
                std::cout << "Closing at request of client" << "\n";
                close(clntSocket);    /* Close client socket */
                return;
            }
        }
    }
    
    std::cout << "Closing" << "\n";
    close(clntSocket);    /* Close client socket */
}
