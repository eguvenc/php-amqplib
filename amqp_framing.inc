<?php

  /**
   * Convert between AMQP frames and higher level methods
   *
   * http://code.google.com/p/php-amqplib/
   * Barry Pederson <bp@barryp.org>
   *
   */

require_once('amqp_basic_message.inc');
require_once('amqp_exceptions.inc');


class PartialMessage
{
    //
    // Helper class to build up a multi-frame method.
    //
    public function __construct($method_sig, $args)
    {
        $this->method_sig = $method_sig;
        $this->args = $args;
        $this->msg = new Message();
        $this->body_parts = array();
        $this->body_received = 0;
        $this->body_size = 0;
        $this->complete = false;
    }

    public function add_header($payload)
    {
        $a = unpack('nnNN', substr($payload, 0, 12));

        // FIXME: body_size is a 64-bit value, we're just
        // assuming the first four bytes are zeros and
        // using the lower 32 bits.  Most likely we're
        // going to be receiving messages <2GB in size
        // so it'll probably never matter.
        $this->body_size = $a[3];

        $this->msg->load_properties(substr($payload, 12));
        $this->complete = ($this->body_size == 0);
    }

    public function add_payload($payload)
    {
        array_push($this->body_parts, $payload);
        $this->body_received += strlen($payload);

        if ($this->body_received == $this->body_size)
        {
            $this->msg->body = implode('', $this->body_parts);
            $this->complete = true;
        }
    }
}


class MethodReader
{
    /*
    Helper class to receive frames from the broker, combine them if
    necessary with content-headers and content-bodies into complete methods.

    Normally a method is represented as a tuple containing
    (channel, method_sig, args, content).

    In the case of a framing error, an AMQPConnectionException is placed
    in the queue.

    In the case of unexpected frames, a tuple made up of
    (channel, AMQPChannelException) is placed in the queue.

    */

    private static $CONTENT_METHODS = array(
        "60,50", // Basic.return
        "60,60", // Basic.deliver
        "60,71", // Basic.get_ok
    );

    public function __construct($source)
    {
        $this->source = $source;
        $this->expected_types = array();
        $this->partial_messages = array();
        $this->result = NULL;
    }

    public function read_method()
    {
        $this->result = NULL;
        while ($this->result == NULL)
        {
            $a = $this->source->read_frame();
            // FIXME: Trying to do something like python tuple-unpacking
            // here, don't know what the PHP equivalent is.
            $frame_type = $a[0];
            $channel = $a[1];
            $payload = $a[2];

            if (!isset($this->expected_types[$channel]))
            {
                $this->expected_types[$channel] = 1;
            }

            if ($this->expected_types[$channel] != $frame_type)
            {
                throw new Exception("Received frame type $frame_type while expecting type: $this->expected_types[$channel]");
            }

            switch ($frame_type)
            {
                case 1:
                    $this->process_method_frame($channel, $payload);
                    break;
                case 2:
                    $this->process_content_header($channel, $payload);
                    break;
                case 3:
                    $this->process_content_body($channel, $payload);
                    break;
            }
        }
        return $this->result;
    }

    protected function process_method_frame($channel, $payload)
    {
        $a = unpack('nx/ny', substr($payload, 0, 4));
        $method_sig = $a['x'] . ',' . $a['y'];
        $args = new AMQPReader(substr($payload, 4));
        debug_msg("process_method_frame, method_sig=$method_sig");
        if (in_array($method_sig, MethodReader::$CONTENT_METHODS))
        {
            //
            // Save what we've got so far and wait for the content-header
            //
            $this->partial_messages[$channel] = new PartialMessage($method_sig, $args);
            $this->expected_types[$channel] = 2;
        }
        else
        {
            $this->result = array($channel, $method_sig, $args, NULL);
        }

    }

    protected function process_content_header($channel, $payload)
    {
        $partial = $this->partial_messages[$channel];
        $partial->add_header($payload);

        if ($partial->complete)
        {
            //
            // a bodyless message, we're done
            //
            $this->result = array($channel, $partial->method_sig, $partial->args, $partial->msg);
            unset($this->partial_messages[$channel]);
            $this->expected_types[$channel] = 1;
        }
        else
        {
            //
            // wait for the content-body
            //
            $this->expected_types[$channel] = 3;
        }
    }

    protected function process_content_body($channel, $payload)
    {
        $partial = $this->partial_messages[$channel];
        $partial->add_payload($payload);

        if ($partial->complete)
        {
            // Save the result and go back to waiting for method
            // frames on this channel.
            $this->result = array($channel, $partial->method_sig, $partial->args, $partial->msg);
            unset($this->partial_messages[$channel]);
            $this->expected_types[$channel] = 1;
        }

    }

}


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
