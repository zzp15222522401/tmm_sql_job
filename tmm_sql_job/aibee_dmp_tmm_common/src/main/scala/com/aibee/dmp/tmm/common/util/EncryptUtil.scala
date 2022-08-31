package com.aibee.dmp.tmm.common.util

import org.apache.commons.codec.binary.Base64

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

object EncryptUtil {
  def encrypt(key: String, value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
    Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(key: String, encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
    new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
  }

  def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  private val SALT: String = "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  def main(args: Array[String]): Unit = {
    //固定该key
    val key = Config.KEY
    //pwd进行加密
    val pwd = "WmjC31tkA07eJ62C"

    val encryptedValue = encrypt(key, pwd)

    println(encryptedValue)

    val str = decrypt(key, "0Mrjz/wDClJekwYte4FUKXY6VX1gbAQ48w9Dz0ZxVEc=")

    println(str)
  }
}
