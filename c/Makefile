CC ?= gcc
CFLAGS ?= -O3 -Wall -Wextra -fPIC -std=c11
LIB_NAME = libtickhub_atomic.so
DEST_DIR = ../python/tickhub

all: $(LIB_NAME)

$(LIB_NAME): tickhub_atomic.c
	$(CC) $(CFLAGS) -shared -o $(LIB_NAME) tickhub_atomic.c
	mkdir -p $(DEST_DIR)
	cp $(LIB_NAME) $(DEST_DIR)/$(LIB_NAME)

clean:
	rm -f $(LIB_NAME) $(DEST_DIR)/$(LIB_NAME)

.PHONY: all clean
