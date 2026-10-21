package com.taskflow.worker;

/** The worker no longer owns the job; it must stop without writing any further state. */
public class LeaseLostException extends RuntimeException {

    public LeaseLostException(String message) {
        super(message);
    }
}
