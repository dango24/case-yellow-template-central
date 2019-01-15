/*
* randompassword
* Written by Mark Henderson (mhender@amazon.com)
* Adapted for use for OS X by Gerrit DeWitt (gerritd@amazon.com)
* 2014-07-21 Copyright Amazon
* Generates a random password per 4.8 in https://policy.amazon.com/standard/143.
*/

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

/* These give 96 bits of entropy which makes for an annoying escrowed password. */
#define RNDBUFSIZ 12
#define PWLEN 16	/* really just RNDBUFSIZ * 4 / 3 */

/* 
* Get rid of annoying character ambiguities, so none of these:
* capital I (India), capital O (Oscar), and lower case l (Lima)
* For special characters, avoid / and $.
*/
unsigned char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                               '+', 'J', 'K', 'L', 'M', 'N', '-', 'P',
                               'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                               'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                               'g', 'h', 'i', 'j', 'k', '*', 'm', 'n',
                               'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                               'w', 'x', 'y', 'z', '0', '1', '2', '3',
                               '4', '5', '6', '7', '8', '9', '@', '#'};

main() {
	int fd,i,j;
	unsigned char rbuf[RNDBUFSIZ];
	unsigned char pw[PWLEN+1];
	if ((fd = open("/dev/urandom", O_RDONLY)) < 0) {
		perror("cannot open /dev/urandom");
		exit(1);
	}
	if (read(fd,&rbuf[0],RNDBUFSIZ,1) < RNDBUFSIZ) {
		fprintf(stderr, "read did not return enough bytes\n");
		exit(1);
	}
	for (i = 0, j = 0; i < RNDBUFSIZ;) {

		unsigned int octet_a = i < RNDBUFSIZ ? rbuf[i++] : 0;
		unsigned int octet_b = i < RNDBUFSIZ ? rbuf[i++] : 0;
		unsigned octet_c = i < RNDBUFSIZ ? rbuf[i++] : 0;

		unsigned int triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

		pw[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
		pw[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
		pw[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
		pw[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
	}
	pw[j] = '\0';  /* null terminate */
	printf("%s", pw);
	exit(0);
}