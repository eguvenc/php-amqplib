<?php
  /**
   * Simple AMQP client library for AMQP for protocol version 0.8
   *
   * http://code.google.com/p/php-amqplib/
   * Vadim Zaliva <lord@crocodile.org>
   *
   */

require_once('amqp_abstract_channel.inc');
require_once('amqp_basic_message.inc');
require_once('amqp_channel.inc');
require_once('amqp_exceptions.inc');
require_once('amqp_framing.inc');
require_once('amqp_wire.inc');

class AMQPConnection extends AbstractChannel
{
    public static $AMQP_PROTOCOL_HEADER = "AMQP\x01\x01\x09\x01";

    public static $LIBRARY_PROPERTIES = array(
        "library" => array('S', "PHP Simple AMQP lib"),
        "library_version" => array('S', "0.1")
    );

    protected static $METHOD_MAP = array(
        "10,10" => "start",
        "10,20" => "secure",
        "10,30" => "tune",
        "10,41" => "open_ok",
        "10,50" => "redirect",
        "10,60" => "_close",
        "10,61" => "close_ok"
    );

    public function __construct($host, $port,
                                $user, $password,
                                $vhost="/",$insist=false,
                                $login_method="AMQPLAIN",
                                $login_response=NULL,
                                $locale="en_US")
    {
        $this->METHOD_MAP = AMQPConnection::$METHOD_MAP;

        if($user && $password)
        {
            $login_response = new AMQPWriter();
            $login_response->write_table(array("LOGIN" => array('S',$user),
                                               "PASSWORD" => array('S',$password)));
            $login_response = substr($login_response->getvalue(),4); //Skip the length
        } else
            $login_response = NULL;


        $d = AMQPConnection::$LIBRARY_PROPERTIES;
        while(true)
        {
            $this->channels = array();
            // The connection object itself is treated as channel 0
            parent::__construct($this, 0);

            $this->channel_max = 65535;
            $this->frame_max = 131072;

            $errstr = $errno = NULL;
            $this->sock = NULL;
            if (!($this->sock = fsockopen($host,$port,$errno,$errstr)))
                throw new Exception ("Error Connecting to server($errno): $errstr ");
            stream_set_blocking($this->sock, 1);
            $this->out = new AMQPWriter($this->sock);
            $this->input = new AMQPReader(null, $this->sock);

            $this->method_reader = new MethodReader($this->input);
            $this->method_writer = new MethodWriter($this->out, $this->frame_max);

            $this->out->write(AMQPConnection::$AMQP_PROTOCOL_HEADER);
            $this->out->flush();
            $this->wait(array("10,10"));
            $this->x_start_ok($d, $login_method, $login_response, $locale);

            $this->wait_tune_ok = true;
            while($this->wait_tune_ok)
            {
                $this->wait(array(
                                "10,20", // secure
                                "10,30", // tune
                            ));
            }

            $host = $this->x_open($vhost,"", $insist);
            if(!$host)
                return; // we weren't redirected

            // we were redirected, close the socket, loop and try again
            debug_msg("closing socket");
            @fclose($this->sock); $this->sock=NULL;
        }
    }

    public function __destruct()
    {
        if(isset($this->input))
            if($this->input)
                $this->close();

        if($this->sock)
        {
            debug_msg("closing socket");
            @fclose($this->sock);
        }
    }

    protected function do_close()
    {
        if(isset($this->input))
            if($this->input)
            {
                $this->input->close();
                $this->input = NULL;
            }

        if($this->sock)
        {
            debug_msg("closing socket");
            @fclose($this->sock);
            $this->sock = NULL;
        }
    }

    public function get_free_channel_id()
    {
        for($i=1;$i<=$this->channel_max;$i++)
            if(!array_key_exists($i,$this->channels))
                return $i;
        throw new Exception("No free channel ids");
    }


    /*
     * Wait for a method from the server destined for
     * a particular channel
     */
    protected function wait_method($channel_id, $allowed_methods)
    {
        //
        // Check the channel's deferred methods
        //
        $method_queue = $this->channels[$channel_id]->method_queue;

        foreach($method_queue as $qk=>$queued_method)
        {
            debug_msg("checking queue method " . $qk);
            $method_sig = $queued_method[0];
            if($allowed_methods==NULL || in_array($method_sig, $allowed_methods))
            {
                unset($method_queue[$qk]);
                debug_msg("Executing queued method: $method_sig: " .
                          $METHOD_NAME_MAP[methodSig($method_sig)]);
                return $queued_method;
            }
        }

        //
        // Nothing queued, need to wait for a method from the peer
        //
        while (true)
        {
            $a = $this->method_reader->read_method();
            // FIXME: trying to do python-style tuple unpacking here, not
            // sure what the PHP equivalent is.
            $channel = $a[0];
            $method_sig = $a[1];
            $args = $a[2];
            $content = $a[3];

            if (($channel == $channel_id)
            and ($allowed_methods==NULL || in_array($method_sig, $allowed_methods)))
            {
                return array($method_sig, $args, $content);
            }

            //
            // Not the channel and/or method we were looking for.  Queue
            // this method for later
            //
            global $METHOD_NAME_MAP;
            debug_msg("Queueing on channel $channel for later: $method_sig: " .
                $METHOD_NAME_MAP[methodSig($method_sig)]);
            array_push($this->channels[$channel]->method_queue,
                array($method_sig, $args, $content));

            //
            // If we just queued up a method for channel 0 (the Connection
            // itself) it's probably a close method in reaction to some
            // error, so deal with it right away.
            //
            if (channel == 0)
            {
                $this->wait();
            }
        }
    }


    /**
     * Fetch a Channel object identified by the numeric channel_id, or
     * create that object if it doesn't already exist.
     */
    public function channel($channel_id=NULL)
    {
        if(array_key_exists($channel_id,$this->channels))
            return $this->channels[$channel_id];

        return new AMQPChannel($this->connection, $channel_id);
    }

    /**
     * request a connection close
     */
    public function close($reply_code=0, $reply_text="", $method_sig=array(0, 0))
    {
        $args = new AMQPWriter();
        $args->write_short($reply_code);
        $args->write_shortstr($reply_text);
        $args->write_short($method_sig[0]); // class_id
        $args->write_short($method_sig[1]); // method_id
        $this->send_method(array(10, 60), $args);
        return $this->wait(array(
                               "10,61",    // Connection.close_ok
                           ));
    }

    public static function dump_table($table)
    {
        $tokens = array();
        foreach ($table as $name => $value)
        {
            switch ($value[0])
            {
                case 'D':
                    $val = $value[1]->n . 'E' . $value[1]->e;
                    break;
                case 'F':
                    $val = '(' . self::dump_table($value[1]) . ')';
                case 'T':
                    $val = date('Y-m-d H:i:s', $value[1]);
                    break;
                default:
                    $val = $value[1];
            }
            $tokens[] = $name . '=' . $val;
        }
        return implode(', ', $tokens);

    }

    protected function _close($args)
    {
        $reply_code = $args->read_short();
        $reply_text = $args->read_shortstr();
        $class_id = $args->read_short();
        $method_id = $args->read_short();

        $this->x_close_ok();

        throw new AMQPConnectionException($reply_code, $reply_text, array($class_id, $method_id));
    }


    /**
     * confirm a connection close
     */
    protected function x_close_ok()
    {
        $this->send_method(array(10, 61));
        $this->do_close();
    }

    /**
     * confirm a connection close
     */
    protected function close_ok($args)
    {
        $this->do_close();
    }

    protected function x_open($virtual_host, $capabilities="", $insist=false)
    {
        $args = new AMQPWriter();
        $args->write_shortstr($virtual_host);
        $args->write_shortstr($capabilities);
        $args->write_bit($insist);
        $this->send_method(array(10, 40), $args);
        return $this->wait(array(
                               "10,41", // Connection.open_ok
                               "10,50"  // Connection.redirect
                           ));
    }


    /**
     * signal that the connection is ready
     */
    protected function open_ok($args)
    {
        $this->known_hosts = $args->read_shortstr();
        debug_msg("Open OK! known_hosts: " . $this->known_hosts);
        return NULL;
    }


    /**
     * asks the client to use a different server
     */
    protected function redirect($args)
    {
        $host = $args->read_shortstr();
        $this->known_hosts = $args->read_shortstr();
        debug_msg("Redirected to [". $host . "], known_hosts [" . $this->known_hosts . "]" );
        return $host;
    }

    /**
     * security mechanism challenge
     */
    protected function secure($args)
    {
        $challenge = $args->read_longstr();
    }

    /**
     * security mechanism response
     */
    protected function x_secure_ok($response)
    {
        $args = new AMQPWriter();
        $args->write_longstr($response);
        $this->send_method(array(10, 21), $args);
    }

    /**
     * start connection negotiation
     */
    protected function start($args)
    {
        $this->version_major = $args->read_octet();
        $this->version_minor = $args->read_octet();
        $this->server_properties = $args->read_table();
        $this->mechanisms = explode(" ", $args->read_longstr());
        $this->locales = explode(" ", $args->read_longstr());

        debug_msg(sprintf("Start from server, version: %d.%d, properties: %s, mechanisms: %s, locales: %s",
                          $this->version_major,
                          $this->version_minor,
                          self::dump_table($this->server_properties),
                          implode(', ', $this->mechanisms),
                          implode(', ', $this->locales)));
    }


    protected function x_start_ok($client_properties, $mechanism, $response, $locale)
    {
        $args = new AMQPWriter();
        $args->write_table($client_properties);
        $args->write_shortstr($mechanism);
        $args->write_longstr($response);
        $args->write_shortstr($locale);
        $this->send_method(array(10, 11), $args);
    }

    /**
     * propose connection tuning parameters
     */
    protected function tune($args)
    {
        $v=$args->read_short();
        if($v)
            $this->channel_max = $v;
        $v=$args->read_long();
        if($v)
            $this->frame_max = $v;
        $this->heartbeat = $args->read_short();

        $this->x_tune_ok($this->channel_max, $this->frame_max, 0);
    }

    /**
     * negotiate connection tuning parameters
     */
    protected function x_tune_ok($channel_max, $frame_max, $heartbeat)
    {
        $args = new AMQPWriter();
        $args->write_short($channel_max);
        $args->write_long($frame_max);
        $args->write_short($heartbeat);
        $this->send_method(array(10, 31), $args);
        $this->wait_tune_ok = False;
    }

}

?>
