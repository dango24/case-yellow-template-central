'use strict';

class IllegalArgumentException extends Error {
    constructor(message) {
        super(message);
        // Capture the stack trace, excluding the constructor call from it.
        Error.captureStackTrace(this, this.constructor);
        // Set the name field to the class name
        this.name = this.constructor.name;
    }
}
exports.IllegalArgumentException = IllegalArgumentException;
