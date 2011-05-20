CXX = g++
CPPFLAGS = -c -O -g -pg -W -Wall -Wextra -pedantic -pipe -MMD -MP
#CPPFLAGS = -c -O3 -W -Wall -Wextra -pedantic -pipe -MMD -MP
LDFLAGS = -lstdc++

SOURCES = mrcoffee.cpp mrcoffee_client.cpp
OBJECTS = $(SOURCES:.cpp=.o)
DEPS = $(SOURCES:.cpp=.d)

all: mrcoffee mrcoffee_client

mrcoffee: mrcoffee.o

mrcoffee_client: mrcoffee_client.o

-include $(DEPS)

clean:
	$(RM) mrcoffee mrcoffee.o mrcoffee_client mrcoffee_client.o

.PHONY: all clean
