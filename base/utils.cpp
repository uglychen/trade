#include "utils.h"

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <openssl/ssl.h>
#include <unistd.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <netinet/in.h>

#include "logging.h"

void die(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  va_list ap2;
  va_copy(ap2, ap);
  std::vector<char> buf(1+std::vsnprintf(NULL, 0, fmt, ap));
  va_end(ap);
  std::vsnprintf(buf.data(), buf.size(), fmt, ap2);
  va_end(ap2);
  LOG(ERROR) << buf.data();

  exit(1);
}

uint64_t now_microseconds(void)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (uint64_t) tv.tv_sec * 1000000 + (uint64_t) tv.tv_usec;
}

void microsleep(int usec)
{
  struct timespec req;
  req.tv_sec = 0;
  req.tv_nsec = 1000 * usec;
  nanosleep(&req, NULL);
}

int encrypt(unsigned char *plaintext, int plaintext_len, unsigned char *key, unsigned char *iv, unsigned char *ciphertext)
{
  EVP_CIPHER_CTX *ctx;

  int len;

  int ciphertext_len;

  if(!(ctx = EVP_CIPHER_CTX_new())) return 0;

  if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_256_cfb8(), NULL, key, iv)) return 0;

  if(1 != EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, plaintext_len)) return 0;
  ciphertext_len = len;

  if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + len, &len)) return 0;
  ciphertext_len += len;

  EVP_CIPHER_CTX_free(ctx);

  return ciphertext_len;
}

int decrypt(unsigned char* ciphertext, int ciphertext_len, unsigned char* key, unsigned char* iv, unsigned char* plaintext)
{
  EVP_CIPHER_CTX *ctx;

  int len;

  int plaintext_len;

  if(!(ctx = EVP_CIPHER_CTX_new())) exit(1);

  if(1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_cfb8(), NULL, key, iv)) exit(1);

  if(1 != EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, ciphertext_len)) exit(1);
  plaintext_len = len;

  if(1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len)) exit(1);
  plaintext_len += len;

  EVP_CIPHER_CTX_free(ctx);

  return plaintext_len;
}

int base64encode(unsigned char* data, int dataLength, char* result, int resultSize) 
{
  const char base64chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  int resultIndex = 0;
  int x;
  uint32_t n = 0;
  int padCount = dataLength % 3;
  uint8_t n0, n1, n2, n3;

  for (x = 0; x < dataLength; x += 3) {
    n = ((uint32_t)data[x]) << 16;

    if((x+1) < dataLength)
      n += ((uint32_t)data[x+1]) << 8;

    if((x+2) < dataLength)
        n += data[x+2];

    n0 = (uint8_t)(n >> 18) & 63;
    n1 = (uint8_t)(n >> 12) & 63;
    n2 = (uint8_t)(n >> 6) & 63;
    n3 = (uint8_t)n & 63;
            
    if(resultIndex >= resultSize) return 0;
    result[resultIndex++] = base64chars[n0];
    if(resultIndex >= resultSize) return 0;
    result[resultIndex++] = base64chars[n1];

    if((x+1) < dataLength) {
      if(resultIndex >= resultSize) return 0;
      result[resultIndex++] = base64chars[n2];
    }

    if((x+2) < dataLength) {
      if(resultIndex >= resultSize) return 0;
      result[resultIndex++] = base64chars[n3];
    }
  }

  if (padCount > 0) { 
    for (; padCount < 3; padCount++) { 
      if(resultIndex >= resultSize) return 0;
      result[resultIndex++] = '=';
    } 
  }
  if(resultIndex >= resultSize) return 0;
  result[resultIndex] = 0;
  return resultIndex;
}

int base64decode (const char *in, int inLen, unsigned char *out, int outSize) 
{ 
  const unsigned char d[] = {
    66,66,66,66,66,66,66,66,66,66,64,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,62,66,66,66,63,52,53,
    54,55,56,57,58,59,60,61,66,66,66,65,66,66,66, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,66,66,66,66,66,66,26,27,28,
    29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
    66,66,66,66,66,66
  };

  const char *end = in + inLen;
  char iter = 0;
  uint32_t buf = 0;
  int len = 0;
  
  while (in < end) {
    unsigned char c = d[(int)*in++];
    
    switch (c) {
    case 64: continue; 
    case 66: return 1;
    case 65:
      in = end;
      continue;
    default:
      buf = buf << 6 | c;
      iter++;
      if (iter == 4) {
        if ((len += 3) > outSize) return 1;
        *(out++) = (buf >> 16) & 255;
        *(out++) = (buf >> 8) & 255;
        *(out++) = buf & 255;
        buf = 0; iter = 0;
      }   
    }
  }
  
  if (iter == 3) {
      if ((len += 2) > outSize) return 1;
      *(out++) = (buf >> 10) & 255;
      *(out++) = (buf >> 2) & 255;
  }
  else if (iter == 2) {
      if (++len > outSize) return 1;
      *(out++) = (buf >> 4) & 255;
  }

  return len;
}

int sha256(unsigned char* data, int data_len, char* md5)
{
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, data, data_len);
  SHA256_Final(hash, &sha256);
  int i = 0;
  for(i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    sprintf(md5 + (i * 2), "%02x", hash[i]);
  }
  return SHA256_DIGEST_LENGTH;
}

int get_cpuid(char* cpuid) 
{
  unsigned int s1 = 0;
  unsigned int s2 = 0;
  asm volatile
  (
    "movl $0x01, %%eax; \n\t"
    "xorl %%edx, %%edx; \n\t"
    "cpuid; \n\t"
    "movl %%edx, %0; \n\t"
    "movl %%eax, %1; \n\t"
    : "=m"(s1), "=m"(s2)
  );

  if (0 == s1 && 0 == s2) {
    return 0;
  }

  sprintf(cpuid, "%08X%08X", htonl(s2), htonl(s1));
  return 1;
}

int get_mac(char* mac) 
{
  struct ifreq ifr;
  struct ifconf ifc;
  char buf[1024];
  int success = 0;

  int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (sock == -1) return 0;

  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = buf;
  if (ioctl(sock, SIOCGIFCONF, &ifc) == -1) return 0;

  struct ifreq* it = ifc.ifc_req;
  const struct ifreq* const end = it + (ifc.ifc_len / sizeof(struct ifreq));

  for (; it != end; ++it) {
    strcpy(ifr.ifr_name, it->ifr_name);
    if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
      if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
        if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
          success = 1;
          break;
        }
      }
    } else { 
      return 0;
    }
  }

  unsigned char mac_address[6];

  if (success) {
    memcpy(mac_address, ifr.ifr_hwaddr.sa_data, 6);
    sprintf(mac, "%02x:%02x:%02x%02x:%02x:%02x", mac_address[0], mac_address[1], mac_address[2], mac_address[3], mac_address[4], mac_address[5]);
    return strlen(mac);
  } else {
    return 0;
  }
}

std::string real_password(std::string& password) {
    int cpuid_len, hash_len, mac_len, plaintext_len, ciphertext_len;
    char cpuid[1024], hash[1024], mac[1023], plaintext[1024], ciphertext[1024];

    cpuid_len = get_cpuid(cpuid);
    if (!cpuid_len) die("error");

    hash_len = sha256((unsigned char*)cpuid, cpuid_len, hash);
    if (hash_len != 32) die("sha256 error");

    mac_len = get_mac(mac);
    if (mac_len != 16) die("error");

    ciphertext_len = base64decode(password.c_str(), password.length(), (unsigned char*)ciphertext, 1024);
    if (!ciphertext_len) die("base64decode error");

    plaintext_len = decrypt((unsigned char*)ciphertext, ciphertext_len, (unsigned char*)hash, (unsigned char*)mac, (unsigned char*)plaintext);
    if (!plaintext_len) die("decrypt error");

    return std::string(plaintext, plaintext_len);
}

std::string HmacSha256Encode(std::string input) {
		
		
		char key[] = "40Ry8q71KXVAKDA2iDWhXKlAEvbr6jbJ";
		unsigned char digest[EVP_MAX_MD_SIZE] = {'\0'};  
		unsigned int digest_len = 0;

		HMAC(EVP_sha256(), key, strlen(key), (unsigned char*)input.c_str(), input.size(), digest, &digest_len);
		char md_str[EVP_MAX_MD_SIZE * 2 + 1];
		for (int i = 0; i < (int)digest_len; i++){
			sprintf(&md_str[i*2], "%02x", (unsigned int)digest[i]);
		}
		std::string str = md_str;
		
		return str;
}
