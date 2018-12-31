#include <stdio.h>      /* for printf() and fprintf() */
#include <sys/socket.h> /* for recv() and send() */
#include <unistd.h>     /* for close() */
#include <iostream>
#include <assert.h>
#include "CalcServer.h"
#include "CalcFramer.hpp"
#include "CalcParser.hpp"
#include <sstream>

using namespace std;

void HandleTCPClient(int clntSocket)
{
	// PUT YOUR CODE HERE
    CalcFramer myCalcFramer;
    CalcParser myCalcParser;
    char my_buffer[4096];
    uint64_t current_val = 0;
    
    for (;;) {
        // get input from recv
        int response = recv(clntSocket, my_buffer, sizeof(my_buffer), 0);
        if (response == 0) {
            std::cout << "No response" << "\n";
            break;
        }
        std::string new_input(my_buffer, response);
        
        // pass input to framer
        myCalcFramer.append(new_input);
        
        while (myCalcFramer.hasMessage()) {
            std::string new_message = myCalcFramer.topMessage();
            myCalcFramer.popMessage();
            // pass message to parser
            CalcInstruction_t my_result = myCalcParser.parse(new_message, current_val);
            current_val = my_result.value;
            // return parser output to client
            if (my_result.should_return) {
                std::ostringstream tempstream;
                tempstream << current_val;
                std::string temp = tempstream.str();
                temp.append("\r\n");
                std::cout << "Sending: " << temp;
                char return_message[1024];
                strcpy(return_message, temp.c_str());
                send(clntSocket, return_message, strlen(return_message), 0);
                current_val = 0;
            }
        }
    }

    std::cout << "Closing" << "\n";
    close(clntSocket);    /* Close client socket */
}
