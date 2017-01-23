<?php
require_once('Log.php'); 

class Loger
{
	private static $m_log = array();

	public static function get($logfile="main.log", $loglevel=PEAR_LOG_DEBUG)
	{
		$LINEFORMAT = '%{timestamp} ['.$_SESSION['amfphp_username'].'] [%{priority}] %{class}::%{function} %{message}';

		if(!array_key_exists($logfile, self::$m_log))
		{
			self::$m_log[$logfile] = Log::singleton('file', APPPATH.'/'.$logfile, '', array('lineFormat' => $LINEFORMAT), $loglevel);
		};

		return self::$m_log[$logfile];
	}
};
