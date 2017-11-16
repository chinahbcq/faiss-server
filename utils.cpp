#include "utils.h"
#include <string.h>
#include <math.h>
bool checkPathExists(std::string &path) {
	if (path.length() < 1) {
		return false;
	}
	if (access(path.c_str(), 0) == -1) {
		return false;
	}
	return true;
}

bool mkFolder(std::string &path) {
	if (path.length() < 1) {
		return false;
	}
	if (access(path.c_str(), 0) == -1) {
		int flag = mkdir(path.c_str(), 0777);	
		return flag == 0;
	}
	return true;
}

int encodeID(char *buf, long id) {
	if (buf == NULL || id < 0) {
		return -1;
	}
	sprintf(buf, "%010ld", id);
	return 0;
}

long decodeID(char *buf) {
	if (buf == NULL || strlen(buf) != FIXLEN) {
		return -1;
	}
	return atoi(buf);
}

float cosine(const float *arr1, const float *arr2, int d) {
	float p1 = 0,p2 = 0,px = 0;
	for (int i = 0; i < d; i++) {
		p1 += arr1[i]*arr1[i];
		p2 += arr2[i]*arr2[i];

		px += arr1[i]*arr2[i];	
	}

	return px / sqrt(p1*p2);
}
