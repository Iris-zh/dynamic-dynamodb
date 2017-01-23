<?php
require_once(INCPATH.'Loger.inc');

class consul
{
	public function get_tables($_baseurl, $_token)
	{
		$l_params = array("token" => $_token, "keys" => "true");
		$l_url = $_baseurl . "dynamic-dynamodb/?" . http_build_query($l_params);

		return json_decode(file_get_contents($l_url), true);
	}

	public function get_table($_baseurl, $_token, $_table)
	{
		$l_params = array("token" => $_token);
		$l_url = $_baseurl . "dynamic-dynamodb/$_table?" . http_build_query($l_params);

		return json_decode(file_get_contents($l_url), true);
	}

	public function get_table_index($_baseurl, $_token, $_table, $_indexname)
	{
		$l_params = array("token" => $_token);
		$l_url = $_baseurl . "dynamic-dynamodb/$_table/index/$_indexname?" . http_build_query($l_params);

		return json_decode(file_get_contents($l_url), true);
	}

	public function get_projects($_baseurl, $_token)
	{
		$l_params = array("token" => $_token);
		$l_url = $_baseurl . "app/deploy/config/?recurse&" . http_build_query($l_params);

		$l_data = json_decode(file_get_contents($l_url), true);
		$l_retVal = array();
		$l_tables = $this->get_tables($_baseurl, $_token);

		foreach($l_data as $l_tmp => $l_appdata)
		{
			$l_appname = basename($l_appdata["Key"]);
			$l_appinfo = json_decode(base64_decode($l_appdata["Value"]), true);

			$l_appTablePrefix = $l_appinfo["tablePrefix"];
			$ll_tables = array();

			foreach($l_tables as $l_tmp => $l_key)
			{
				$l_key = explode('/', $l_key);
				$l_table = $l_key[1];

				if (strpos($l_table, $l_appTablePrefix) === 0)
				{
					if (!array_key_exists($l_table, $ll_tables))
					{
						$ll_tables[$l_table] = array();
					};

					if(count($l_key) == 4)
					{
						array_push($ll_tables[$l_table], $l_key[3]);
					};
				};
			};

			$l_retVal[$l_appname] = (count($ll_tables)> 0)?$ll_tables:NULL;
		}

		return $l_retVal;
	}

	public function save_table($_baseurl, $_token, $_table, $_newTableConfigStr)
	{
		$l_tableConfig = $this->get_table($_baseurl, $_token, $_table);
		$l_tableConfig = json_decode(base64_decode($l_tableConfig[0]["Value"]), true);
		$l_newTableConfig = json_decode($_newTableConfigStr, true);

		foreach($l_tableConfig as $l_key => $l_val)
		{
			if(is_array($l_val))
			{
				$l_newTableConfig[$l_key] = $l_val;
			};
		};

		$l_params = array("token" => $_token);
		$l_url = $_baseurl . "dynamic-dynamodb/$_table?" . http_build_query($l_params);

		$curl = curl_init($l_url);
		curl_setopt($curl, CURLOPT_CUSTOMREQUEST, "PUT");
		curl_setopt($curl, CURLOPT_HEADER, false);
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($l_newTableConfig));
		$l_response = curl_exec($curl);

		if (!$l_response)
		{
			throw new Exception("Connection Failure.");
		};

		return true;
	}

	public function save_table_index($_baseurl, $_token, $_table, $_indexname, $l_newTableIndexConfigStr)
	{
		$l_tableindexConfig = $this->get_table_index($_baseurl, $_token, $_table, $_indexname);
		$l_tableindexConfig = json_decode(base64_decode($l_tableindexConfig[0]["Value"]), true);
		$l_newTableIndexConfig = json_decode($l_newTableIndexConfigStr, true);

		foreach($l_tableindexConfig as $l_key => $l_val)
		{
			if(is_array($l_val))
			{
				$l_newTableIndexConfig[$l_key] = $l_val;
			};
		};

		$l_params = array("token" => $_token);
		$l_url = $_baseurl . "dynamic-dynamodb/$_table/index/$_indexname?" . http_build_query($l_params);

		$l_curl = curl_init($l_url);
		curl_setopt($l_curl, CURLOPT_CUSTOMREQUEST, "PUT");
		curl_setopt($l_curl, CURLOPT_HEADER, false);
		curl_setopt($l_curl, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($l_curl, CURLOPT_POSTFIELDS, json_encode($l_newTableIndexConfig));
		$l_response = curl_exec($l_curl);

		if (!$l_response)
		{
			throw new Exception("Connection Failure.");
		};

		return true;
	}

	public function beforeFilter($methodName)
	{
		return Authenticate::isAuthenticated();
	}
}
