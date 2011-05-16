<?php

namespace Predis;

class RedisClusterException extends ServerException implements IReplyObject {
    public function getMoveArguments() {
        return explode(' ', $this->getMessage(), 3);
    }
}
