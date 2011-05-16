<?php

namespace Predis;

class ServerException extends PredisException implements IReplyObject {
    public function __toString() {
        return $this->getMessage();
    }

    public function getRedisErrorType() {
        $message = $this->getMessage();
        return substr($message, 0, strpos($message, ' '));
    }
}
