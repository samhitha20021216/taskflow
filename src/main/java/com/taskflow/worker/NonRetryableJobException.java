package com.taskflow.worker;

/** Thrown by a handler when retrying cannot help (bad input, permanent failure). */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
