<?php

namespace Predis;

class ServerException extends PredisException implements IReplyObject {
    public function getErrorType() {
        list($errorType, ) = explode(' ', $this->getMessage(), 2);
        return $errorType;
    }
}
