<?php

  /**
   * Convert between AMQP frames and higher level methods
   *
   * http://code.google.com/p/php-amqplib/
   * Barry Pederson <bp@barryp.org>
   *
   */

require_once('amqp_exceptions.inc');

class MethodWriter
{
    public function __construct($dest, $frame_max)
    {
        $this->dest = $dest;
        $this->frame_max = $frame_max;
    }

    public function write_method($channel, $method_sig, $args, $content=NULL)
    {
        global $METHOD_NAME_MAP;
        debug_msg("< write_method channel: $channel, method: " . methodSig($method_sig) . '/' . $METHOD_NAME_MAP[methodSig($method_sig)] . ', args: length=' . strlen($args) . " hex=\n" . hexdump($args, $htmloutout=false, $uppercase=true, $return=true));

        $payload = pack('nn', $method_sig[0], $method_sig[1]) . $args;
        $this->dest->write_frame(1, $channel, $payload);


        if ($content != NULL)
        {
            $body = $content->body;
            $payload = pack('nn', $method_sig[0], 0);
            $payload .= implode("", AMQPWriter::chrbytesplit(strlen($body),8));
            $payload .= $content->serialize_properties();

            $this->dest->write_frame(2, $channel, $payload);

            while (strlen($body) > 0)
            {
                $payload = substr($body, 0, $this->frame_max);
                $body = substr($body, $this->frame_max);

                $this->dest->write_frame(3, $channel, $payload);
            }
        }
    }
}
