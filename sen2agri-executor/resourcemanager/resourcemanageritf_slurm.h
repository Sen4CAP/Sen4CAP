#ifndef RessourceManagerItf_SLURM_H
#define RessourceManagerItf_SLURM_H

#include "abstractresourcemanageritf.h"


/**
 * @brief The RessourceManagerItf_SLURM class
 * \note
 * This class represents the interface with the Ressource Manager (SLURM).
 * It takes a wraper and executes the SLURM commands like srun, SSTAT, SACCT etc.)
 */
class ResourceManagerItf_SLURM : public AbstractResourceManagerItf
{

public:
    ResourceManagerItf_SLURM();
    ~ResourceManagerItf_SLURM();

protected:
    virtual bool HandleJobOperations(RequestParamsJobOps *pReqParams);
    virtual bool HandleStartSteps(RequestParamsSubmitSteps *pReqParams);
    virtual void HandleStopTasks(RequestParamsCancelTasks *pReqParams);
    virtual void FillEndStepAddExecInfos(const QString &strJobName, ProcessorExecutionInfos &info);

    bool GetSacctResults(QString &sacctCmd, QList<ProcessorExecutionInfos> &procExecResults);
};

#endif // RessourceManagerItf_SLURM_H
