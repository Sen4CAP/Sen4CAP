install(FILES   ../../../scripts/s4s_perm_crops/s4s-perm-crops-extract-inputs.py
                ../../../scripts/s4s_perm_crops/s4s-perm-crops-build-refl-stack.py
                ../../../scripts/s4s-perm-crops-rasterization.py
                ../../../scripts/s4s-perm-crops-run-broceliande.py
        DESTINATION usr/share/sen2agri/S4S_Permanent_Crops/Bin
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_EXECUTE
                    WORLD_READ WORLD_EXECUTE)
