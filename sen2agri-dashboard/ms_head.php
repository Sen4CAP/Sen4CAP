	<div id="header" style="height:120px;">
	<?php 
	require_once('ConfigParams.php');
    $profileName = ConfigParams::getProfileName();
	if($profileName == "sen2agri")
	{?>
       <div id="header2" style="background-image: url(images/logo5.png); background-repeat: no-repeat;">&nbsp;</div>
       <?php 
    }else
    {?>
       <div id="header2" style="background-image: url(images/logo_<?=$profileName?>.png); background-repeat: no-repeat;">&nbsp;</div> 
       <?php 
    }?>
    </div>
	