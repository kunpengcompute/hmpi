#include "ad_oceanfs.h"
#include "adioi.h"

struct ADIOI_Fns_struct ADIO_OCEANFS_operations = {
    ADIOI_OCEANFS_Open,                /* Open */
    ADIOI_GEN_OpenColl,                /* OpenColl */
    ADIOI_OCEANFS_ReadContig,          /* ReadContig */
    ADIOI_OCEANFS_WriteContig,         /* WriteContig */
    ADIOI_OCEANFS_ReadStridedColl,     /* ReadStridedColl */
    ADIOI_OCEANFS_WriteStridedColl,    /* WriteStridedColl */
    ADIOI_GEN_SeekIndividual,          /* SeekIndividual */
    ADIOI_OCEANFS_Fcntl,               /* Fcntl */
    ADIOI_OCEANFS_SetInfo,             /* SetInfo */
    ADIOI_OCEANFS_ReadStrided,         /* ReadStrided */
    ADIOI_GEN_WriteStrided,            /* WriteStrided */
    ADIOI_OCEANFS_Close,               /* Close */
    ADIOI_FAKE_IreadContig,            /* IreadContig */
    ADIOI_FAKE_IwriteContig,           /* IwriteContig */
    ADIOI_FAKE_IODone,                 /* ReadDone */
    ADIOI_FAKE_IODone,                 /* WriteDone */
    ADIOI_FAKE_IOComplete,             /* ReadComplete */
    ADIOI_FAKE_IOComplete,             /* WriteComplete */
    ADIOI_FAKE_IreadStrided,           /* IreadStrided */
    ADIOI_FAKE_IwriteStrided,          /* IwriteStrided */
    ADIOI_GEN_Flush,                   /* Flush */
    ADIOI_OCEANFS_Resize,              /* Resize */
    ADIOI_GEN_Delete,                  /* Delete */
    ADIOI_GEN_Feature,                 /* Feature */
    ADIOI_OCEANFS_PREFIX,
    ADIOI_GEN_IreadStridedColl,        /* IreadStridedColl */
    ADIOI_GEN_IwriteStridedColl        /* IwriteStridedColl */
};
