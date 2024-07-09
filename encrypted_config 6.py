# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 13:24:31 2023

@author: 120004044
"""

from cryptography.fernet import Fernet

# Generate a key
key = Fernet.generate_key()

# Create a Fernet cipher using the key
cipher_suite = Fernet(key)

# Open and read the config.ini file
with open('config.ini', 'rb') as file:
    plaintext = file.read()

# Encrypt the plaintext using the cipher
ciphertext = cipher_suite.encrypt(plaintext)

# Write the encrypted data to an encrypted_config.ini file
with open('encrypted_config.ini', 'wb') as file:
    file.write(ciphertext)

# Save the key to a separate file for future decryption
with open('encryption_key.key', 'wb') as file:
    file.write(key)