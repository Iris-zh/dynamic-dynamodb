<?php
require_once(INCPATH.'Loger.inc');

//-----------------------------------------------------------------------------
class myauth
{
	const BINDUSERDN = "cn=ldap,cn=users,dc=playnet,dc=local";
	const BINDUSERPWD = "oBnYl8L4V1";

	public function login($login, $password)
	{
		$retval = false;

		if (!Authenticate::isAuthenticated())
		{
			$l_ldapconn = ldap_connect("ldap.playrix.local",3268);

			if(!$l_ldapconn)
			{
				Loger::get("route.error.log")->err("Failed to create ldap resource");
				throw new Exception("Failed to create ldap resource");
			};

			ldap_set_option($l_ldapconn, LDAP_OPT_PROTOCOL_VERSION, 3);
			ldap_set_option($l_ldapconn, LDAP_OPT_REFERRALS, 0);

			if(@ldap_bind($l_ldapconn, self::BINDUSERDN, self::BINDUSERPWD))
			{
				//$l_sr = ldap_search($l_ldapconn, "cn=users,dc=playnet,dc=local", "(&(cn=$login)(|(memberOf=CN=Domain Admins,CN=Users,DC=playnet,DC=local)(memberOf=CN=d_rukovod,CN=Users,DC=playnet,DC=local)))", array('sAMAccountName'));
				$l_sr = ldap_search($l_ldapconn, "dc=playnet,dc=local", sprintf("(sAMAccountName=%s)", $this->__ldap_escape($login, false)));

				if(!$l_sr)
				{
					Loger::get("apps.audit.log")->err(ldap_error($l_ldapconn));
					throw new Exception("Failed to search ldap");
				};

				$l_result = ldap_get_entries($l_ldapconn, $l_sr);
				$isValidUser = $l_result['count'] != 0;//test if valid user

				if($l_result['count'])
				{
					if(!empty($password))
					{
						$l_user_result = $l_result[0];
						$l_user_dn = $l_user_result["dn"];
						Loger::get("apps.audit.log")->info("user dn: ".$l_user_dn);

						if(ldap_bind($l_ldapconn, $l_user_dn, $password))
						{
							Loger::get("apps.audit.log")->info("user $login login succsess");

							Authenticate::login($login, "admin"); //todo:handle roles
							$retval = true;
						}
						else
						{
							Loger::get("apps.audit.log")->err("user $login login failed due: ".ldap_error($l_ldapconn));
						};
					}
					else
					{
						Loger::get("apps.audit.log")->err("password is empty for user : ".$login);
					}
				}
				else
				{
					Loger::get("apps.audit.log")->err("Can't find user with login: $login");
				};
			}
			else
			{
				Loger::get("apps.audit.log")->err("Can't bind to ldap due: ".ldap_error($l_ldapconn));
			};
		};

		return $retval;
	}

	//http://stackoverflow.com/questions/8560874/php-ldap-add-function-to-escape-ldap-special-characters-in-dn-syntax
	private function __ldap_escape($subject, $dn = FALSE, $ignore = NULL)
	{
		// The base array of characters to escape
		// Flip to keys for easy use of unset()
		$search = array_flip($dn ? array('\\', ',', '=', '+', '<', '>', ';', '"', '#') : array('\\', '*', '(', ')', "\x00"));

		// Process characters to ignore
		if (is_array($ignore))
		{
			$ignore = array_values($ignore);
		}

		for ($char = 0; isset($ignore[$char]); $char++)
		{
			unset($search[$ignore[$char]]);
		}

		// Flip $search back to values and build $replace array
		$search = array_keys($search); 
		$replace = array();

		foreach ($search as $char)
		{
			$replace[] = sprintf('\\%02x', ord($char));
		}

		// Do the main replacement
		$result = str_replace($search, $replace, $subject);

		// Encode leading/trailing spaces in DN values
		if ($dn)
		{
		    if ($result[0] == ' ')
		    {
		        $result = '\\20'.substr($result, 1);
		    }

		    if ($result[strlen($result) - 1] == ' ')
		    {
		        $result = substr($result, 0, -1).'\\20';
		    };
		};

		return $result;
	}

	public function logout()
	{
	}
};
