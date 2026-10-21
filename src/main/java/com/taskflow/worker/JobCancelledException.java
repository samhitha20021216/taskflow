package com.taskflow.worker;

public class JobCancelledException extends RuntimeException {

    public JobCancelledException(String message) {
        super(message);
    }
}
