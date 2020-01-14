#ifndef UTILS_H_
#define UTILS_H_

#include <stdint.h>
#include <string>

void die(const char *fmt, ...);

uint64_t now_microseconds(void);
void microsleep(int usec);

int encrypt(unsigned char *plaintext, int plaintext_len, unsigned char *key, unsigned char *iv, unsigned char *ciphertext);
int decrypt(unsigned char* ciphertext, int ciphertext_len, unsigned char* key, unsigned char* iv, unsigned char* plaintext);
int base64encode(unsigned char* data, int data_len, char* result, int resultSize);
int base64decode (const char *in, int inLen, unsigned char *out, int outSize);
int sha256(unsigned char* data, int data_len, char* md5);
int get_cpuid(char* cpuid);
int get_mac(char* mac);
std::string real_password(std::string& password);

std::string HmacSha256Encode(std::string input);

#endif  // UTILS_H_