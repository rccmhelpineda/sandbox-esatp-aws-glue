import os
import time

def generate_uuidv7():

    """
    **Generates a strictly compliant UUIDv7 
    (Timestamp first, random second)
    """

    t_ms = int(time.time() * 1000)
    t_hex = f"{t_ms:012x}"

    # 12-bit random a + version 7
    r_a = os.urandom(2).hex()
    r_a = f"7{r_a[1:]}"

    # 62-bit random b + variant 10
    r_b = os.urandom(8).hex()
    r_b = f"{hex(int(r_b[0], 16) & 0x3 | 0x8)[2:]}{r_b[1:]}"
    
    return f"{t_hex[:8]}-{t_hex[8:]}-{r_a}-{r_b[:4]}-{r_b[4:]}"