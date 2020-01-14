#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "utils.h"

void valid(char* password, int password_len);

int main (int argc, char** argv) {
    int cpuid_len, hash_len, mac_len, text_len, ciphertext_len, base64_len;
    char cpuid[1024];
    char hash[1024];
    char mac[1023];
    char text[1024];
    char ciphertext[1024];
    char base64[1024];

    scanf("%s", text);
    text_len = strlen(text);
    if (!text_len) die("no input");

    cpuid_len = get_cpuid(cpuid);
    if (!cpuid_len) die("error");

    hash_len = sha256((unsigned char*)cpuid, cpuid_len, hash);
    if (hash_len != 32) die("hash error");

    mac_len = get_mac(mac);
    if (mac_len != 16) die("error");

    ciphertext_len = encrypt((unsigned char*)text, text_len, (unsigned char*)hash, (unsigned char*)mac, (unsigned char*)ciphertext);
    if (!ciphertext_len) die("encrypt error");

    base64_len = base64encode((unsigned char*)ciphertext, ciphertext_len, base64, 1024);
    printf("%s\n", base64);

    return 0;
}

void valid(char* password, int password_len) {
    int cpuid_len, hash_len, mac_len, plaintext_len, ciphertext_len;
    char cpuid[1024], hash[1024], mac[1023], plaintext[1024], ciphertext[1024];

    cpuid_len = get_cpuid(cpuid);
    if (!cpuid_len) die("error");

    hash_len = sha256((unsigned char*)cpuid, cpuid_len, hash);
    if (hash_len != 32) die("sha256 error");

    mac_len = get_mac(mac);
    if (mac_len != 16) die("error");

    ciphertext_len = base64decode(password, password_len, (unsigned char*)ciphertext, 1024);
    if (!ciphertext_len) die("base64decode error");

    plaintext_len = decrypt((unsigned char*)ciphertext, ciphertext_len, (unsigned char*)hash, (unsigned char*)mac, (unsigned char*)plaintext);
    if (!plaintext_len) die("decrypt error");

    plaintext[plaintext_len] = 0;
    printf("%s\n", plaintext);
}
