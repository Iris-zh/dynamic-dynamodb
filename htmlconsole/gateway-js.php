<?php
require_once("_defines.inc");
require_once("amfphp/json-rpc/app/Gateway.php");

//-----------------------------------------------------------------------------
session_start();

//-----------------------------------------------------------------------------
$gateway = new Gateway();
$servicesPath = SRVPATH;

$gateway->setBaseClassPath($servicesPath);
$gateway->service();
