CXX = g++
CPPFLAGS = -c -O -g -pg -W -Wall -Wextra -pedantic -std=c++0x -pipe -MMD -MP
#CPPFLAGS = -c -O3 -W -Wall -Wextra -pedantic -pipe -MMD -MP
LDFLAGS = -lstdc++

SOURCES = mrcoffee.cpp mrcoffee_client.cpp io.cpp
OBJECTS = $(SOURCES:.cpp=.o)
DEPS = $(SOURCES:.cpp=.d)

all: mrcoffee mrcoffee_client

mrcoffee: mrcoffee.o io.o

mrcoffee_client: mrcoffee_client.o io.o

-include $(DEPS)

clean:
	$(RM) mrcoffee mrcoffee_client $(OBJECTS) $(DEPS)

.PHONY: all clean
