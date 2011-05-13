<?php

namespace Predis\Commands;

abstract class ScriptedCommand extends ServerEval {
    private static $_sha1Cache = array();
    private $_commandId;

    public function __construct() {
        $script = $this->getScript();
        if (isset(self::$_sha1Cache[$script])) {
            $this->_commandId = 'EVALSHA';
        }
        else {
            $this->_commandId = 'EVAL';
            self::$_sha1Cache[$script] = sha1($script);
        }
    }

    public function getId() {
        return $this->_commandId;
    }

    public abstract function getScript();

    protected function getFirstArgument() {
        $script = $this->getScript();
        return $this->getId() === 'EVALSHA' ? self::$_sha1Cache[$script] : $script;
    }

    protected function keysCount() {
        // The default behaviour for the base class is to use all the arguments
        // passed to a scripted command to populate the KEYS table in Lua.
        return count($this->getArguments());
    }

    protected function filterArguments(Array $arguments) {
        return array_merge(array($this->getFirstArgument(), $this->keysCount()), $arguments);
    }

    protected function getKeys() {
        return array_slice($this->getArguments(), 2, $this->keysCount());
    }
}
